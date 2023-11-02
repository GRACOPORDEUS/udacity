import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# Drop existing tables
def drop_tables(cur, conn):
    """
    Drops existing tables in the database.

    Args:
        cur (psycopg2.cursor): The database cursor.
        conn (psycopg2.connection): The database connection.

    Returns:
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

# Create new tables
def create_tables(cur, conn):
    """
    Creates new tables in the database.

    Args:
        cur (psycopg2.cursor): The database cursor.
        conn (psycopg2.connection): The database connection.

    Returns:
        None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

# Main function to orchestrate table creation and deletion
def main():
    """
    Main function to orchestrate the creation and deletion of tables in the database.
    It connects to the database, drops existing tables, and creates new ones.

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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()
