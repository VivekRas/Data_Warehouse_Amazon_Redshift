import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function reads all the "drop table if exists" queries from SQL_queries.py and executes them
    """    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function reads all the "create table" queries from SQL_queries.py and executes them
    Creating tables as per star schema with Dimension and fact tables
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This function is the main fns, we use it to 
    a) establish a connection with AWS Redshift - Postgres Data base
    b) call all the other fns one after the other and finally close the connection
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":# the create_tables.py program, starts here and gets redirected to main fns
    main()