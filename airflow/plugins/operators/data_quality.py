from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow import AirflowException


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 queries=[{"query": None, "expected_result_function": None}],
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.queries = queries


    def execute(self, context):
        """
        For each combination of query and validation function in self.queries, 
        this function runs the query and parses the result of the query into the validation function
        and returns the function verdict

        Raises:
            AirflowException: Raises an exception if the verdict is false
        """
        self.log.info("Getting Redshift hook")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for element in self.queries:
            try:
                self.log.info("Running validation query against Redshift")
                records = redshift.get_records(element['query'])
                self.log.info("Validation result locally")
                verdict = element['expected_result_function'].__call__(records)

                if not verdict:
                    err = f"Values for query does not match expected result: {element['query']}"
                    raise ValueError(err)
            except Exception as e:
                err = f"DataQualityOperator Error: {e}"
                self.log.info(err)
                raise AirflowException(err)

        self.log.info('DataQualityOperator successfully completed')
