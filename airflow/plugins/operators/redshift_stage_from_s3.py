from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageFromS3ToRedshiftOperator(BaseOperator):
    # Custom Operator: https://airflow.readthedocs.io/en/stable/howto/custom-operator.html

    ui_color = '#358140'

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        FORMAT AS json '{}'
        TRUNCATECOLUMNS 
        BLANKSASNULL
        EMPTYASNULL;
    """

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 s3_bucket="",
                 s3_key="",
                 table="",
                 region="",
                 json="",
                 *args, **kwargs):
        super(StageFromS3ToRedshiftOperator, self).__init__(*args, **kwargs)
        # Parameter mappings
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.region = region
        self.json = json
        #self.execution_date = kwargs.get('execution_date')


    def execute(self, context):
        """
        Copies data from S3 buckets to redshift cluster into staging tables.
        """
        self.log.info("Getting AWS hooks")
        aws_hook = AwsHook(self.aws_credentials_id)

        self.log.info("Loading AWS credentials")
        credentials = aws_hook.get_credentials()
        
        self.log.info("Getting Redshift hook")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)        
        
        self.log.info("Clearing data from destination table")
        redshift.run(f"TRUNCATE TABLE {self.table};")
        
        self.log.info("Copying {self.table} from S3 to Redshift")
        s3_path = f"s3://{self.s3_bucket}/{self.s3_key.format(**context)}"

        formatted_sql = self.copy_sql.format(   
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json
        )
        redshift.run(formatted_sql)

        self.log.info(f"Completed copying {self.table} from S3 to Redshift")
