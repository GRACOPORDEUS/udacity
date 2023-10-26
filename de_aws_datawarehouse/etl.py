import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

# Load data into staging tables from S3 buckets
def load_staging_tables(cur, conn):
    """
    Loads data into staging tables from S3 buckets.

    Args:
        cur (psycopg2.cursor): The database cursor.
        conn (psycopg2.connection): The database connection.

    Returns:
        None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

# Insert data from staging tables into analytics tables
def insert_tables(cur, conn):
    """
    Inserts data from staging tables into analytics tables.

    Args:
        cur (psycopg2.cursor): The database cursor.
        conn (psycopg2.connection): The database connection.

    Returns:
        None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

# Main function to orchestrate the ETL process
def main():
    """
    Main function to orchestrate the ETL (Extract, Transform, Load) process.
    It connects to the database, loads data into staging tables, and inserts it into analytics tables.

    Args:
        None

    Returns:
        None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        host=config.get("CLUSTER", "HOST"),
        dbname=config.get("CLUSTER", "DB_NAME"),
        user=config.get("CLUSTER", "DB_USER"),
        password=config.get("CLUSTER", "DB_PASSWORD"),
        port=config.get("CLUSTER", "DB_PORT")
    )
    cur = conn.cursor()

    #load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()