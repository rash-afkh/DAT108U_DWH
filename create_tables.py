# import the required libraries
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# function to drop all tables
def drop_tables(cur, conn):
    for table_name, query in drop_table_queries.items():
        print(f"\tDropping {table_name} table")  # print a message about the table being dropped
        cur.execute(query)  # execute the DROP TABLE query
        conn.commit()  # commit the transaction after dropping the table

# function to create all tables
def create_tables(cur, conn):
    for table_name, query in create_table_queries.items():
        print(f"\tCreating {table_name} table")  # print a message about the table being created
        cur.execute(query)  # execute the CREATE TABLE query
        conn.commit()  # commit the transaction after creating the table


# main function
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')  # read the configuration file
    
    # establish connection to the database using the configuration values
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print('Dropping tables ...')  # print a message about dropping tables
    drop_tables(cur, conn)  # call the function to drop tables
    
    print('Creating tables ...')  # print a message about creating tables
    create_tables(cur, conn)  # call the function to create tables

    conn.close()  # close the database connection

# execute the main function if this script is run as the main module
if __name__ == "__main__":
    main()
