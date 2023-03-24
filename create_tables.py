import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
'''
this function is going to loop all the sql queries in the drop_table_queries and run it, make sure we clean these tables if we already have these tables.
cur is the cursor.
conn is the connection, so we can connect to the redshift cluster.
'''    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
'''
this function is going to loop all the sql queries in the create_table_queries and run it to generate these tables.
cur is the cursor.
conn is the connection, so we can connect to the redshift cluster
''' 
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
'''
this function read the cluster setups from the dwh.cfg file.
then connect to the redshift cluster, at last do the drop and create tables works.
''' 
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()