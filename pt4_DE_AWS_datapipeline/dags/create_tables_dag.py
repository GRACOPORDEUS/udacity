import pendulum
from airflow.operators.empty import EmptyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.decorators import dag
import sql_statements

@dag(start_date=pendulum.now(),
     max_active_runs=1)
def create_tables():
    """
    DAG to create tables in a Redshift database.

    This DAG defines tasks to create various tables in a Redshift database using SQL statements.

    Tasks:
    - create_artists_table
    - create_songplays_table
    - create_songs_table
    - create_time_table
    - create_users_table
    - create_staging_events_table
    - create_staging_songs_table
    """
    
    create_artists_table = PostgresOperator(
        task_id='create_artists_table',
        postgres_conn_id='redshift',
        sql=sql_statements.CREATE_ARTISTS_TABLE_SQL
    )

    create_songplays_table = PostgresOperator(
        task_id='create_songplays_table',
        postgres_conn_id='redshift',
        sql=sql_statements.CREATE_SONGPLAYS_TABLE_SQL
    )

    create_songs_table = PostgresOperator(
        task_id='create_songs_table',
        postgres_conn_id='redshift',
        sql=sql_statements.CREATE_SONGS_TABLE_SQL
    )

    create_time_table = PostgresOperator(
        task_id='create_time_table',
        postgres_conn_id='redshift',
        sql=sql_statements.CREATE_TIME_TABLE_SQL
    )

    create_users_table = PostgresOperator(
        task_id='create_users_table',
        postgres_conn_id='redshift',
        sql=sql_statements.CREATE_USERS_TABLE_SQL
    )

    create_staging_events_table = PostgresOperator(
        task_id='create_staging_events_table',
        postgres_conn_id='redshift',
        sql=sql_statements.CREATE_STAGING_EVENTS_TABLE_SQL
    )

    create_staging_songs_table = PostgresOperator(
        task_id='create_staging_songs_table',
        postgres_conn_id='redshift',
        sql=sql_statements.CREATE_STAGING_SONGS_TABLE_SQL
    )

create_tables_dag = create_tables()

