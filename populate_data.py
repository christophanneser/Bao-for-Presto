"""This module populates data from other sources (e.g. postgres) to presto and a predefined catalog (e.g. hive)"""
from presto_connector import get_session

TPCH_TABLES = ['part', 'supplier', 'nation', 'region', 'customer', 'partsupp', 'lineitem', 'orders']
IMDB_TABLES = ['aka_name', 'aka_title', 'cast_info', 'char_name', 'comp_cast_type', 'company_name', 'company_type', 'complete_cast', 'info_type', 'keyword',
               'kind_type', 'link_type', 'movie_companies', 'movie_info', 'movie_info_idx', 'movie_keyword', 'movie_link', 'name', 'person_info', 'role_type',
               'title']
STACKOVERFLOW_TABLES = ['account', 'answer', 'badge', 'comment', 'post_link', 'question', 'site', 'so_user', 'tag', 'tag_question']
# todo taxi dataset

# DWRF does not support dates
CREATE_LINEITEM = """CREATE TABLE IF NOT EXISTS {0}.{1}.lineitem AS
    SELECT orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax,
   returnflag, linestatus, cast(shipdate as varchar) as shipdate, cast(commitdate as varchar) as commitdate,
   cast(receiptdate as varchar) as receiptdate, shipinstruct, shipmode, comment,
   linestatus = 'O' as is_open, returnflag = 'R' as is_returned, 
   cast(tax as real) as tax_as_real, cast(discount as real) as discount_as_real, 
   cast(linenumber as smallint) as linenumber_as_smallint, 
   cast(linenumber as tinyint) as linenumber_as_tinyint 
  FROM {2}.lineitem"""

CREATE_ORDERS = """CREATE TABLE IF NOT EXISTS {0}.{1}.orders AS
    SELECT orderkey, custkey, orderstatus, totalprice, cast(orderdate as varchar) as orderdate, 
    orderpriority, clerk, shippriority, comment 
    FROM {2}.orders"""


def execute(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    print(query)
    result = cursor.fetchall()
    print(result)


def gen_data(source_schema, target_catalog, target_schema, tables):
    session = get_session()
    session.set_catalog(target_catalog)
    session.set_schema(target_schema)
    connection = session.get_connection()
    execute(connection, 'CREATE SCHEMA IF NOT EXISTS {0}.{1}'.format(target_catalog, target_schema))
    execute(connection, 'USE {0}.{1}'.format(target_catalog, target_schema))

    for table in tables:
        cmd = 'CREATE TABLE IF NOT EXISTS {0}.{1}.{2} AS SELECT * FROM {3}.{2}'
        cmd = cmd.format(target_catalog, target_schema, table, source_schema)
        execute(connection, cmd)


# when generating data using worker other than the coordinator, mount /tmp/presto-data on each worker
if __name__ == '__main__':
    gen_data('postgresql.public', 'hive', 'stackoverflow', STACKOVERFLOW_TABLES)
    # gen_imdb('postgresql.public', 'hive', 'imdb')
    # gen_tpch('tpch.sf1', 'hive', 'tpch_sf1')
    # gen_tpch('tpch.sf10', 'hive', 'tpch_sf10')
    # gen_tpch('tpch.sf100', 'hive', 'tpch_sf100')
