from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class RedshiftExecuteSQLOperator(BaseOperator):
    
    ui_color = '#b7bee8'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 *args, **kwargs):
        super(RedshiftExecuteSQLOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query


    def execute(self, context):
        self.log.info("Getting Redshift hook")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Running SQL statements on Redshift")
        sql_commands = self.sql_query.split(';')
        for command in sql_commands:
            if command.rstrip() != '':
                redshift.run(command)
                
        self.log.info("SQL statements executed successfully")