from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Operator for loading data into a Redshift dimensional table.

    This operator supports two modes:
    - 'delete-load': Deletes existing data in the table before loading new data.
    - 'append': Appends new data to the existing data in the table.

    :param redshift_conn_id: The connection ID for the Redshift database.
    :type redshift_conn_id: str
    :param table: The name of the target table.
    :type table: str
    :param sql_query: The SQL query for loading data into the table.
    :type sql_query: str
    :param mode: The mode of loading data ('delete-load' or 'append').
    :type mode: str
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 sql_query='',
                 mode='delete-load',
                 *args, **kwargs):
        """
        Initializes the LoadDimensionOperator.

        :param redshift_conn_id: The connection ID for the Redshift database.
        :type redshift_conn_id: str
        :param table: The name of the target table.
        :type table: str
        :param sql_query: The SQL query for loading data into the table.
        :type sql_query: str
        :param mode: The mode of loading data ('delete-load' or 'append').
        :type mode: str
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.mode = mode

    def execute(self, context):
        """
        Executes the loading of data into the specified Redshift dimensional table.

        :param context: The context passed by Airflow during execution.
        :type context: dict
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.mode == 'delete-load':
            self.log.info("\n\n### Deleting data from Dimensional Table")
            redshift.run(f"TRUNCATE {self.table}")

        self.log.info(f'\n\n### Loading Dimensional Table')
        redshift.run(f"INSERT INTO {self.table} {self.sql_query}")
