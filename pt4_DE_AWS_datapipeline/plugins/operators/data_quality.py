from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Operator for performing data quality checks on a Redshift database.

    This operator executes SQL queries to perform both count and null checks on specified tables.

    :param redshift_conn_id: The connection ID for the Redshift database.
    :type redshift_conn_id: str
    :param data_quality_count_checks: List of SQL queries for count checks.
    :type data_quality_count_checks: list
    :param data_quality_null_checks: List of SQL queries for null checks.
    :type data_quality_null_checks: list
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 data_quality_count_checks=[],
                 data_quality_null_checks=[],
                 *args, **kwargs):
        """
        Initializes the DataQualityOperator.

        :param redshift_conn_id: The connection ID for the Redshift database.
        :type redshift_conn_id: str
        :param data_quality_count_checks: List of SQL queries for count checks.
        :type data_quality_count_checks: list
        :param data_quality_null_checks: List of SQL queries for null checks.
        :type data_quality_null_checks: list
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_quality_count_checks = data_quality_count_checks
        self.data_quality_null_checks = data_quality_null_checks

    def execute(self, context):
        """
        Executes data quality checks on the specified Redshift database.

        :param context: The context passed by Airflow during execution.
        :type context: dict
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # records = should be a list with a single tuple
        # containing a single element: the counting value
        self.log.info('\n\n############ DATA QUALITY COUNT CHECKS ############')
        for query in self.data_quality_count_checks:
            records = redshift.get_records(query)

            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                self.log.error(f"Data quality check failed. {query} returned no results")
                raise ValueError(f"Data quality check failed. {query} returned no results")

            self.log.info(f"Data quality on query {query} check passed with {records[0][0]} records\n\n")

        # records = should be a list with a single tuple
        # containing a single element: the counting value equals to 0: [(0,)]
        # If it was not, it means that there are records with NULL values
        self.log.info('\n\n############ DATA QUALITY NULL CHECKS ############')
        for query in self.data_quality_null_checks:
            records = redshift.get_records(query)

            if len(records) > 1 or len(records[0]) > 1 or records[0][0] > 0:
                self.log.info(records)
                self.log.error(f"Data quality check failed. {query} returned results")
                raise ValueError(f"Data quality check failed. {query} returned results")

            self.log.info(f"Data quality on query {query} check for NULL values passed with non-NULL values\n\n")
