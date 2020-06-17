from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=None,
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables


    def execute(self, context):
        self.log.info("Getting Redshift hook")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('TODO: Implement data quality checks!')

        self.log.info('DataQualityOperator successfully completed')
