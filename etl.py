import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This fns is used to 
    Read data from AWS S3 and load it to the staging tables on AWS Redshift - Postgres using the copy command
    
    Parameters: 
    cur : Cursor
    conn : Connection
    
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This fns is used to 
    Read data from the two staging tables and insert it into the respective dimension and fact tables
    
    Parameters: 
    cur : Cursor
    conn : Connection
    
    """    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This function is the main fns, we use it to 
    a) establish a connection with AWS Redshift - Postgres Data base
    b) call all the other fns, one after the other and finally close the connection
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":# the etl.py program, starts here and gets redirected to main fns
    main()