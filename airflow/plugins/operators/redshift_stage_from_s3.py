from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageFromS3ToRedshiftOperator(BaseOperator):
    # Custom Operator: https://airflow.readthedocs.io/en/stable/howto/custom-operator.html

    ui_color = '#358140'

    copy_csv_sql = """
        COPY {} {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        DELIMITER ','
        CSV;
    """

    copy_json_sql = """
        COPY {} {}
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
                 json_format="",
                 file_type="json",
                 column_names="",
                 *args, **kwargs):
        super(StageFromS3ToRedshiftOperator, self).__init__(*args, **kwargs)
        # Parameter mappings
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.region = region
        self.json_format = json_format
        self.file_type = file_type
        self.column_names = column_names
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
        
        self.log.info(f"Copying data to {self.table} from S3 to Redshift")
        s3_path = f"s3://{self.s3_bucket}/{self.s3_key.format(**context)}"

        formatted_sql = self.get_sql(self.file_type, s3_path, credentials)
        redshift.run(formatted_sql)

        self.log.info(f"Completed copying data to {self.table} from S3 to Redshift")


    def get_sql(self, file_type, s3_path, credentials): 
        """
        Gets the correct SQL for the 

        Args:
            file_type (string): json, csv supported

        Returns:
            string: COPY SQL for the selected file_type 
        """
        switcher = { 
            "json": self.copy_json_sql.format(   
                self.table,
                self.column_names,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.region,
                self.json_format
            ), 
            "csv": self.copy_csv_sql.format(   
                self.table,
                self.column_names,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.region
            ), 
        } 
    
        # get() method of dictionary data type returns  
        # value of passed argument if it is present  
        # in dictionary otherwise second argument will 
        # be assigned as default value of passed argument 
        return switcher.get(file_type, "json") # json default 
