import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for table_name, query in copy_table_queries.items():
        print(f"\tLoading {table_name} table")
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for table_name, query in insert_table_queries.items():   
        print(f"\tInserting to {table_name} table") 
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('Loading staging tables ...')
    load_staging_tables(cur, conn)
    
    print('Inserting data into tables ...')
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()