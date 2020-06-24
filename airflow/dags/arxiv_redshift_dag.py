import os
import helpers
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator 
from airflow.operators import (RedshiftExecuteSQLOperator,
                               StageFromS3ToRedshiftOperator,
                               LoadRedshiftTableOperator,
                               DataQualityOperator)


# Default arguments for DAG with arguments as specified by Project Specification

default_args = {
    'owner': 'ArXiv',
    'start_date': datetime(2020, 6, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}


# DAG object creation
# Scheduler: https://airflow.apache.org/docs/stable/scheduler.html

dag = DAG(
    'arXiv_Redshift_dag',
    default_args=default_args,
    description='Load and transform data from S3 in Redshift with Airflow',
    schedule_interval='@once', 
    catchup=True,
    max_active_runs=1
)
dag.doc_md = """
### DAG Summary
This DAG describes the ETL process for ArXiv data from S3 to Redshift

### Points of Contact
Email: jacob@jbmadsen.com
"""


# Task Operators

start_operator = DummyOperator(
    task_id='Begin_execution',  
    dag=dag
)
start_operator.doc_md = """
#Dummy operator
"""

create_staging_tables_redshift = RedshiftExecuteSQLOperator(
    task_id='create_staging_tables',
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift",
    sql_query=helpers.RedshiftSqlQueries.create_staging_tables
)
create_staging_tables_redshift.doc_md = """
#Dummy operator
"""

stage_metadata_to_redshift = StageFromS3ToRedshiftOperator(
    task_id='stage_metadata',
    dag=dag,
    provide_context=True,
    table="staging.metadata",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="arxiv-etl",
    s3_key="staging/metadata",
    region="us-east-1",
    json_format="auto",
    file_type="json"
)
stage_metadata_to_redshift.doc_md = """
#Dummy operator
"""

re_parse_authors_data = PythonOperator(
    task_id='re_parse_authors',
    dag=dag,
    provide_context=True,
    python_callable=helpers.load_authors,
    op_kwargs={
        'aws_credentials_id': 'aws_credentials', 
        'redshift_connection_id': 'redshift',
        's3_credentials_id': 's3_credentials', 
        'region': 'us-east-1', 
        'bucket': 'arxiv-etl', 
        'file_name': 'staging/authors/authors-parsed.json'
    }, 
)
re_parse_authors_data.doc_md = """
#Dummy operator
"""

stage_authors_to_redshift = StageFromS3ToRedshiftOperator(
    task_id='stage_authors',
    dag=dag,
    provide_context=True,
    table="staging.authors",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="arxiv-etl",
    s3_key="staging/authors/authors_parsed.csv",
    region="us-east-1",
    file_type="csv"
)
stage_authors_to_redshift.doc_md = """
#Dummy operator
"""


re_parse_citations_data = PythonOperator(
    task_id='re_parse_citations',
    dag=dag,
    provide_context=True,
    python_callable=helpers.load_citations,
    op_kwargs={
        'aws_credentials_id': 'aws_credentials', 
        'redshift_connection_id': 'redshift',
        's3_credentials_id': 's3_credentials', 
        'region': 'us-east-1', 
        'bucket': 'arxiv-etl', 
        'file_name': 'staging/citations/internal-citations.json'
    }, 
)
re_parse_citations_data.doc_md = """
#Dummy operator
"""

stage_citations_to_redshift = StageFromS3ToRedshiftOperator(
    task_id='stage_citations',
    dag=dag,
    provide_context=True,
    table="staging.citations",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="arxiv-etl",
    s3_key="staging/citations/citations_parsed.csv",
    region="us-east-1",
    file_type="csv"
)
stage_citations_to_redshift.doc_md = """
#Dummy operator
"""

stage_classifications_to_redshift = StageFromS3ToRedshiftOperator(
    task_id='stage_classifications',
    dag=dag,
    provide_context=True,
    table="staging.classifications",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="arxiv-etl",
    s3_key="staging/classifications",
    region="us-east-1",
    file_type="csv"
)
stage_classifications_to_redshift.doc_md = """
#Dummy operator
"""

create_main_tables_redshift = RedshiftExecuteSQLOperator(
    task_id='create_main_tables',
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift",
    sql_query=helpers.RedshiftSqlQueries.create_main_tables
)
create_main_tables_redshift.doc_md = """
#Dummy operator
"""

load_articles_table = LoadRedshiftTableOperator(
    task_id='load_articles_fact_table',
    dag=dag, 
    provide_context=True,
    truncate_table=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    table="public.articles_fact",
    sql_query=helpers.RedshiftSqlQueries.insert_articles_fact
)
load_articles_table.doc_md = """
#Dummy operator
"""

# load_article_version_dimension_table = RedshiftExecuteSQLOperator(
#     task_id='load_article_version_dim_table',
#     dag=dag
# )
# load_article_version_dimension_table.doc_md = """
# #Dummy operator
# """

# load_article_categories_dimension_table = RedshiftExecuteSQLOperator(
#     task_id='load_article_categories_dim_table',
#     dag=dag
# )
# load_article_categories_dimension_table.doc_md = """
# #Dummy operator
# """

# load_article_authors_dimension_table = RedshiftExecuteSQLOperator(
#     task_id='load_authors_dim_table',
#     dag=dag
# )
# load_article_authors_dimension_table.doc_md = """
# #Dummy operator
# """

# load_article_authors_bridge_table = RedshiftExecuteSQLOperator(
#     task_id='load_article_authors_bridge_table',
#     dag=dag
# )
# load_article_authors_bridge_table.doc_md = """
# #Dummy operator
# """

# load_article_citations_dimension_table = RedshiftExecuteSQLOperator(
#     task_id='load_article_citations_dim_table',
#     dag=dag
# )
# load_article_citations_dimension_table.doc_md = """
# #Dummy operator
# """

# load_article_classifications_dimension_table = RedshiftExecuteSQLOperator(
#     task_id='load_article_classifications_dim_table',
#     dag=dag
# )
# load_article_classifications_dimension_table.doc_md = """
# #Dummy operator
# """

# load_classifications_dimension_table = RedshiftExecuteSQLOperator(
#     task_id='load_classifications_dim_table',
#     dag=dag
# )
# load_classifications_dimension_table.doc_md = """
# #Dummy operator
# """

# run_quality_checks = DataQualityOperator(
#     task_id='run_quality_checks',
#     dag=dag
# )
# run_quality_checks.doc_md = """
# #Dummy operator
# """

# end_operator = DummyOperator(
#     task_id='Stop_execution',  
#     dag=dag
# )
# end_operator.doc_md = """
# #Dummy operator
# """


# Task Dependencies

start_operator >> [create_staging_tables_redshift,
                   re_parse_authors_data,
                   re_parse_citations_data]

create_staging_tables_redshift >> [stage_metadata_to_redshift,
                                   re_parse_authors_data >> stage_authors_to_redshift,
                                   re_parse_citations_data >> stage_citations_to_redshift,
                                   stage_classifications_to_redshift] >> create_main_tables_redshift

create_main_tables_redshift >> load_articles_table

# load_articles_table >> [load_article_version_dimension_table, 
#                         load_article_categories_dimension_table, 
#                         load_article_authors_dimension_table,
#                         load_article_authors_bridge_table,
#                         load_article_citations_dimension_table,
#                         load_article_classifications_dimension_table,
#                         load_classifications_dimension_table] >> run_quality_checks

# run_quality_checks >> end_operator

# End
