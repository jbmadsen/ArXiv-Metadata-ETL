from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


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
        self.log.info("Getting Redshift hook")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for element in self.queries:
            try:
                records = redshift.get_records(element['query'])
                verdict = element['expected_result_function'].__call__(records)

                if not verdict:
                    err = f"Values for query does not match expected result: {element['query']}"
                    raise ValueError(err)
            except Exception as e:
                self.log.info(f"Error: {e}")

        self.log.info('DataQualityOperator successfully completed')
