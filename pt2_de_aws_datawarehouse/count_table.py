import configparser
import psycopg2
from sql_queries import count_table_queries

def execute_and_print_query(cur, query, message="Query Result:"):
    """
    Executes an SQL query and prints its result.

    Args:
        cur (psycopg2.cursor): The database cursor.
        query (str): The SQL query to execute.
        message (str): Optional message to print before the result.

    Returns:
        None
    """
    cur.execute(query)
    result = cur.fetchall()
    print(message)
    for row in result:
        print(row)

def main():
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

    # Example: execute a query and print the result
    for query in count_table_queries:
        print(query)
        execute_and_print_query(cur, query)

    conn.close()

if __name__ == "__main__":
    main()