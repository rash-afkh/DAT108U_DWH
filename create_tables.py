import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for table_name, query in drop_table_queries.items():
        print(f"\tDropping {table_name} table")
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for table_name, query in create_table_queries.items():
        print(f"\tCreating {table_name} table")
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print('Dropping tables ...')
    drop_tables(cur, conn)
    
    print('Creating tables ...')
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()