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
    'retry_delay': timedelta(minutes=2),
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


############################
# Task Operators
############################

start_operator = DummyOperator(
    task_id='begin_execution',  
    dag=dag
)
start_operator.doc_md = """
# Dummy operator: Start of DAG
"""

# Create tables

create_staging_tables_redshift = RedshiftExecuteSQLOperator(
    task_id='create_staging_tables',
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift",
    sql_query=helpers.RedshiftSqlQueries.create_staging_tables
)
create_staging_tables_redshift.doc_md = """
# Creates staging schema if it does not exists, with default parameters.
# Drops and (re)create all staging tables needed for this DAG.
"""

create_main_tables_redshift = RedshiftExecuteSQLOperator(
    task_id='create_main_tables',
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift",
    sql_query=helpers.RedshiftSqlQueries.create_main_tables
)
create_main_tables_redshift.doc_md = """
# Creates table within existing public schema.
# Drops and (re)create all public tables needed for this DAG.
"""


# Load staged data

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
    file_type="json",
)
stage_metadata_to_redshift.doc_md = """
# Loads data into Redshift staging table from S3
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
# Parses data from S3 locally and re-formats it to easily work with Redshift COPY, then saves it back to S3
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
# Loads data into Redshift staging table from S3
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
# Parses data from S3 locally and re-formats it to easily work with Redshift COPY, then saves it back to S3
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
# Loads data into Redshift staging table from S3
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
# Loads data into Redshift staging table from S3
"""


# Staged data quality checks

quality_check_staged_metadata = DataQualityOperator(
    task_id='quality_check_staged_metadata',  
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    queries=[
        {"query": helpers.RedshiftStagedValidationQueries.MetadataFirstRowsQuery, "expected_result_function": helpers.DataValidationChecks.ValidateNoEmptyColumnsInResult},
    ]
)
quality_check_staged_metadata.doc_md = """
# Runs quality checks and validation scripts on data as described in queries
"""

quality_check_staged_authors = DataQualityOperator(
    task_id='quality_check_staged_authors',  
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    queries=[
        {"query": helpers.RedshiftStagedValidationQueries.AuthorsFirstRowsQuery, "expected_result_function": helpers.DataValidationChecks.ValidateNoEmptyColumnsInResult},
    ]
)
quality_check_staged_authors.doc_md = """
# Runs quality checks and validation scripts on data as described in queries
"""

quality_check_staged_citations = DataQualityOperator(
    task_id='quality_check_staged_citations',  
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    queries=[
        {"query": helpers.RedshiftStagedValidationQueries.CitationsFirstRowsQuery, "expected_result_function": helpers.DataValidationChecks.ValidateNoEmptyColumnsInResult},
    ]
)
quality_check_staged_citations.doc_md = """
# Runs quality checks and validation scripts on data as described in queries
"""

quality_check_staged_classifications = DataQualityOperator(
    task_id='quality_check_staged_classifications',  
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    queries=[
        {"query": helpers.RedshiftStagedValidationQueries.ClassificationsFirstRowsQuery, "expected_result_function": helpers.DataValidationChecks.ValidateNoEmptyColumnsInResult},
    ]
)
quality_check_staged_classifications.doc_md = """
# Runs quality checks and validation scripts on data as described in queries
"""


# Transform to main data

stage_to_main_tables = DummyOperator(
    task_id='transform_to_main_tables',  
    dag=dag
)
stage_to_main_tables.doc_md = """
# Runs quality checks and validation scripts on data as described in queries
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
# Loads and transforms data into fact table from staging tables
"""

load_article_version_dimension_table = LoadRedshiftTableOperator(
    task_id='load_article_version_dim_table',
    dag=dag, 
    provide_context=True,
    truncate_table=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    table="public.versions_dim",
    sql_query=helpers.RedshiftSqlQueries.insert_versions_dim
)
load_article_version_dimension_table.doc_md = """
# Loads and transforms data into dimensional table from staging tables
"""

load_article_authors_dimension_table = LoadRedshiftTableOperator(
    task_id='load_authors_dim_table',
    dag=dag, 
    provide_context=True,
    truncate_table=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    table="public.authors_dim",
    sql_query=helpers.RedshiftSqlQueries.insert_authors_dim
)
load_article_authors_dimension_table.doc_md = """
# Loads and transforms data into dimensional table from staging tables
"""

load_article_classifications_dimension_table = LoadRedshiftTableOperator(
    task_id='load_article_categories_dim_table',
    dag=dag, 
    provide_context=True,
    truncate_table=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    table="public.classifications_dim",
    sql_query=helpers.RedshiftSqlQueries.insert_classifications_dim
)
load_article_classifications_dimension_table.doc_md = """
# Loads and transforms data into dimensional table from staging tables
"""

load_article_citations_dimension_table = LoadRedshiftTableOperator(
    task_id='load_article_citations_dim_table',
    dag=dag, 
    provide_context=True,
    truncate_table=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    table="public.citations_dim",
    sql_query=helpers.RedshiftSqlQueries.insert_citations_dim
)
load_article_citations_dimension_table.doc_md = """
# Loads and transforms data into dimensional table from staging tables
"""


# Main data quality checks

run_quality_checks = DummyOperator( 
    task_id='run_final_quality_checks',
    dag=dag
)
run_quality_checks.doc_md = """
# Dummy operator - starts sequence of data quality checks for main tables
"""

quality_check_articles_fact = DataQualityOperator(
    task_id='quality_check_articles_fact',  
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    queries=[
        {"query": helpers.RedshiftMainValidationQueries.ArticleFactFirstRowsQuery, "expected_result_function": helpers.DataValidationChecks.ValidateNoEmptyColumnsInResult},
        {"query": helpers.RedshiftMainValidationQueries.CanJoinFactAndAllDims, "expected_result_function": helpers.DataValidationChecks.ResultsExists},
    ]
)
quality_check_articles_fact.doc_md = """
# Runs quality checks and validation scripts on data as described in queries
"""

quality_check_versions_dim = DataQualityOperator(
    task_id='quality_check_versions_dim',  
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    queries=[
        {"query": helpers.RedshiftMainValidationQueries.VersionsDimFirstRowsQuery, "expected_result_function": helpers.DataValidationChecks.ValidateNoEmptyColumnsInResult},
    ]
)
quality_check_versions_dim.doc_md = """
# Runs quality checks and validation scripts on data as described in queries
"""

quality_check_authors_dim = DataQualityOperator(
    task_id='quality_check_authors_dim',  
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    queries=[
        {"query": helpers.RedshiftMainValidationQueries.AuthorsDimFirstRowsQuery, "expected_result_function": helpers.DataValidationChecks.ValidateNoEmptyColumnsInResult},
    ]
)
quality_check_authors_dim.doc_md = """
# Runs quality checks and validation scripts on data as described in queries
"""

quality_check_classifications_dim = DataQualityOperator(
    task_id='quality_check_classifications_dim',  
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    queries=[
        {"query": helpers.RedshiftMainValidationQueries.ClassificationsDimFirstRowsQuery, "expected_result_function": helpers.DataValidationChecks.ValidateNoEmptyColumnsInResult},
    ]
)
quality_check_classifications_dim.doc_md = """
# Runs quality checks and validation scripts on data as described in queries
"""

quality_check_citations_dim = DataQualityOperator(
    task_id='quality_check_citations_dim',  
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    queries=[
        {"query": helpers.RedshiftMainValidationQueries.CitationsDimFirstRowsQuery, "expected_result_function": helpers.DataValidationChecks.ValidateNoEmptyColumnsInResult},
    ]
)
quality_check_citations_dim.doc_md = """
# Runs quality checks and validation scripts on data as described in queries
"""


# End

end_operator = DummyOperator(
    task_id='stop_execution',  
    dag=dag
)
end_operator.doc_md = """
# Dummy operator - marks end of DAG
"""


############################
# Task Dependencies
############################

start_operator >> [create_staging_tables_redshift,
                   create_main_tables_redshift,
                   re_parse_authors_data,
                   re_parse_citations_data]

create_staging_tables_redshift >> [stage_metadata_to_redshift,
                                   re_parse_authors_data >> stage_authors_to_redshift,
                                   re_parse_citations_data >> stage_citations_to_redshift,
                                   stage_classifications_to_redshift]

[stage_metadata_to_redshift >> quality_check_staged_metadata,
 stage_authors_to_redshift >> quality_check_staged_authors,
 stage_citations_to_redshift >> quality_check_staged_citations,
 stage_classifications_to_redshift >> quality_check_staged_classifications,
 create_main_tables_redshift] >> stage_to_main_tables

stage_to_main_tables >> [load_articles_table,
                         load_article_version_dimension_table,
                         load_article_authors_dimension_table,
                         load_article_classifications_dimension_table,
                         load_article_citations_dimension_table] >> run_quality_checks

run_quality_checks >> [quality_check_articles_fact,
                      quality_check_versions_dim,
                      quality_check_authors_dim,
                      quality_check_classifications_dim,
                      quality_check_citations_dim] >> end_operator

# End
