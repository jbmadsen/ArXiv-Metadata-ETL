from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadRedshiftTableOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 truncate_table=False,
                 *args, **kwargs):

        super(LoadRedshiftTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.truncate_table = truncate_table
        
        
    def execute(self, context):
        self.log.info("Getting Redshift hook")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_table:
            self.log.info(f"Truncating {self.table} before inserting")
            redshift.run(f"TRUNCATE TABLE {self.table};")
            self.log.info(f"Successfully truncated {self.table} on Redshift")

        self.log.info(f"Inserting into {self.table} on Redshift")
        redshift.run(self.sql_query)
        self.log.info(f"Successfully inserted into {self.table} on Redshift")
