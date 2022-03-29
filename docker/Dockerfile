FROM ubuntu:latest

RUN apt-get update && apt-get upgrade -y
RUN apt-get update && apt-get upgrade -y
RUN apt-get install openjdk-8-jdk python3 graphviz libpq-dev python3-dev -y
RUN apt-get install python3.9-venv python3-pip -y
RUN apt-get install postgresql -y

# Setup presto coordinator
COPY presto-server.tar.gz presto-server.tar.gz
RUN mkdir presto-server && \
    tar -xf presto-server.tar.gz -C presto-server --strip-components=1 && \
    rm presto-server.tar.gz && \
    mkdir -p presto-server/etc

# Driver code
COPY ./bao.tar.gz bao.tar.gz
RUN mkdir driver && \
    tar -xf bao.tar.gz -C driver && \
    rm bao.tar.gz && \
    cd driver && \
    pip3 install -r requirements.txt --find-links https://download.pytorch.org/whl/torch_stable.html

COPY presto-config /presto-config
RUN cd presto-server && rm -rf etc && ln -sf /presto-config etc

RUN apt-get install vim -y

RUN createdb bao_presto_benchmark
ENTRYPOINT python3 presto-server/bin/launcher.py run
