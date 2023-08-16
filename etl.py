# Import necessary libraries
import configparser  # Library for reading configuration files
import psycopg2  # Library for PostgreSQL database connection
from sql_queries import copy_table_queries, insert_table_queries  # Custom SQL queries

# Function to load data into staging tables
def load_staging_tables(cur, conn):
    for table_name, query in copy_table_queries.items():
        print(f"\tLoading {table_name} table...")  # Print status message
        cur.execute(query)  # Execute the COPY command to load data into staging table
        conn.commit()  # Commit the transaction after loading data
        print(f"\t\t{table_name} table loaded.")  # Print success message

# Function to insert data into analytical tables
def insert_tables(cur, conn):
    for table_name, query in insert_table_queries.items():
        print(f"\tInserting data into {table_name} table...")  # Print status message
        cur.execute(query)  # Execute the INSERT INTO command to insert data
        conn.commit()  # Commit the transaction after inserting data
        print(f"\t\tData inserted into {table_name} table.")  # Print success message


# Main function
def main():
    # Read the database configuration from the 'dwh.cfg' file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # Establish a connection to the database using the configuration values
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('Loading staging tables...')  # Print status message
    load_staging_tables(cur, conn)  # Call the function to load staging tables
    
    print('Inserting data into tables...')  # Print status message
    insert_tables(cur, conn)  # Call the function to insert data into analytical tables

    # Close the database connection
    conn.close()
    print('Database connection closed.')


# Execute the main function if this script is run as the main module
if __name__ == "__main__":
    main()
