from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Operator for loading data into a Redshift fact table.

    This operator is designed for appending data to an existing fact table.

    :param redshift_conn_id: The connection ID for the Redshift database.
    :type redshift_conn_id: str
    :param table: The name of the target table.
    :type table: str
    :param sql_query: The SQL query for loading data into the table.
    :type sql_query: str
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 sql_query='',
                 *args, **kwargs):
        """
        Initializes the LoadFactOperator.

        :param redshift_conn_id: The connection ID for the Redshift database.
        :type redshift_conn_id: str
        :param table: The name of the target table.
        :type table: str
        :param sql_query: The SQL query for loading data into the table.
        :type sql_query: str
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        """
        Executes the loading of data into the specified Redshift fact table.

        :param context: The context passed by Airflow during execution.
        :type context: dict
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f'\n\n### Loading Fact Table')
        redshift.run(f"INSERT INTO {self.table} {self.sql_query}")
