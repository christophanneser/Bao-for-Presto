"""This module provides several helper functions to connect to presto server and to receive messages from Presto via the predefined bao-socket"""
import prestodb
from optimizer_config import QuerySpan
import json
import struct
import socketserver
from enum import Enum
from custom_logging import logging

# expected presto message prefixes
OPTIMIZERS = 'optimizers:'
RULES = 'rules:'
EFFECTIVE = 'effective:'
REQUIRED = 'required:'
LOGICAL = 'logical:'
FRAGMENTED = 'fragmented:'

# message encodings
JSON = 'json:'
DOT = 'dot:'


class PrestoOptimizerType(Enum):
    OPTIMIZER = 1
    RULE = 2


def recvall(sock, size, buffer=bytes()):
    while len(buffer) < size:
        part = sock.recv(size - len(buffer))
        buffer += part
    return buffer


def remove_prefix(text, prefix):
    assert text.startswith(prefix)
    return text[len(prefix):]


def receive_query_span(message, optimizer_type):
    assert optimizer_type in [PrestoOptimizerType.OPTIMIZER, PrestoOptimizerType.RULE]
    if message.startswith(EFFECTIVE):
        if optimizer_type == PrestoOptimizerType.RULE:
            get_session().status.query_span.effective_rules = json.loads(remove_prefix(message, EFFECTIVE))
        else:
            get_session().status.query_span.effective_optimizers = json.loads(remove_prefix(message, EFFECTIVE))
    else:
        assert message.startswith(REQUIRED)
        if optimizer_type == PrestoOptimizerType.RULE:
            get_session().status.query_span.required_rules = json.loads(remove_prefix(message, REQUIRED))
        else:
            get_session().status.query_span.required_optimizers = json.loads(remove_prefix(message, REQUIRED))


class PrestoCallbackHandler(socketserver.BaseRequestHandler):
    """Request handle class to receive and decode all the messages from presto server."""

    def get_message(self):
        buffer = recvall(self.request, 4)
        length = struct.unpack('>i', buffer[:4])[0]
        buffer = recvall(self.request, length + 4, buffer)
        return buffer[4:].decode()

    def handle(self):
        """there are different types of messages:
        1. Query span containing either rules or optimizers (encoded as json)
        2. Logical or fragmented query plan (encoded as json)
        3. Query execution stats (e.g. time measurements, plan hash, etc.)
        4. Graphviz encoded query plan (encoded as dot)
        """
        message = self.get_message()

        if message.startswith(JSON):
            message = remove_prefix(message, JSON)
            message = message.strip()
            if message.startswith(RULES):
                receive_query_span(remove_prefix(message, RULES), PrestoOptimizerType.RULE)
            elif message.startswith(OPTIMIZERS):
                receive_query_span(remove_prefix(message, OPTIMIZERS), PrestoOptimizerType.OPTIMIZER)
            elif message.startswith(LOGICAL):
                get_session().status.logical_json = json.loads(remove_prefix(message, LOGICAL))
            elif message.startswith(FRAGMENTED):
                get_session().status.fragmented_json = json.loads(remove_prefix(message, FRAGMENTED))
            else:
                execution_stats = json.loads(message)
                get_session().status.execution_stats = execution_stats
                get_session().status.recorded_stats[execution_stats['query_id']] = execution_stats
        elif message.startswith(DOT):
            dot = remove_prefix(message, DOT)
            if dot.startswith(LOGICAL):
                self.server.logical_dot = remove_prefix(dot, LOGICAL)
            elif dot.startswith(FRAGMENTED):
                self.server.fragmented_dot = remove_prefix(dot, FRAGMENTED)
        self.request.close()


class PrestoSession:
    """This class wraps a session to presto as well as a callback server communicating with presto"""

    class Status:
        """Store the information received from presto"""

        def __init__(self):
            self.query_span = QuerySpan()
            self.execution_stats = dict()
            self.logical_dot = None
            self.fragmented_dot = None
            self.logical_json = None
            self.fragmented_json = None
            self.allow_reuse_address = True
            self.recorded_stats = dict()

    def __init__(self, catalog=None, schema=None, request_timeout=prestodb.constants.DEFAULT_REQUEST_TIMEOUT, execution_timeout='4m'):
        self.connection = prestodb.dbapi.connect(
            host='localhost',
            port=8080,
            user='admin',
            catalog=catalog,
            schema=schema,
            request_timeout=request_timeout,
            session_properties={'query_max_execution_time': execution_timeout},
        )
        self.callback_server = socketserver.TCPServer(('localhost', 9999), PrestoCallbackHandler)
        self.status = PrestoSession.Status()
        self.callback_server.eprsdf = self

    def get_connection(self):
        return self.connection

    def set_catalog(self, catalog):
        self.connection.catalog = catalog

    def set_schema(self, schema):
        self.connection.schema = schema

    def restart_callback_server(self):
        # close previous callback server, pending requests will be discarded
        if self.callback_server is not None:
            logging.info('shutdown callback server to discard previous messages')
            self.callback_server.server_close()

        logging.info('start callback server')
        self.callback_server = socketserver.TCPServer(('localhost', 9999),
                                                      PrestoCallbackHandler)
        self.callback_server.session = self
        self.status = PrestoSession.Status()


presto_session = None


def get_session():
    global presto_session
    if presto_session is None:
        presto_session = PrestoSession()
    return presto_session
