import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
'''
this function is going to loop all the sql queries in the copy_table_queries and run it to finish the copy process.
cur is the cursor.
conn is the connection, so we can connect to the redshift cluster.
''' 
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
'''
this function is going to loop all the sql queries in the insert_table_queries and run it to insert data into these tables.
cur is the cursor.
conn is the connection, so we can connect to the redshift cluster.
''' 
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
'''
this function read the cluster setups from the dwh.cfg file.
then connect to the redshift cluster, at last do the copy and insert tables works.
''' 
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()