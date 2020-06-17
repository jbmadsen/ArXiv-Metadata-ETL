import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (RedshiftExecuteSQLOperator,
                               StageFromS3ToRedshiftOperator,
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
    'arXiv_dag',
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
    dag=dag
)
create_staging_tables_redshift.doc_md = """
#Dummy operator
"""

stage_metadata_to_redshift = StageFromS3ToRedshiftOperator(
    task_id='stage_metadata',
    dag=dag
)
stage_metadata_to_redshift.doc_md = """
#Dummy operator
"""

stage_authors_to_redshift = StageFromS3ToRedshiftOperator(
    task_id='stage_authors',
    dag=dag
)
stage_authors_to_redshift.doc_md = """
#Dummy operator
"""

stage_citations_to_redshift = StageFromS3ToRedshiftOperator(
    task_id='stage_citations',
    dag=dag
)
stage_citations_to_redshift.doc_md = """
#Dummy operator
"""

stage_classifications_to_redshift = StageFromS3ToRedshiftOperator(
    task_id='stage_classifications',
    dag=dag
)
stage_classifications_to_redshift.doc_md = """
#Dummy operator
"""

create_main_tables_redshift = RedshiftExecuteSQLOperator(
    task_id='create_main_tables',
    dag=dag
)
create_main_tables_redshift.doc_md = """
#Dummy operator
"""

load_articles_table = RedshiftExecuteSQLOperator(
    task_id='load_articles_fact_table',
    dag=dag, 
    provide_context=True
)
load_articles_table.doc_md = """
#Dummy operator
"""

load_article_version_dimension_table = RedshiftExecuteSQLOperator(
    task_id='load_article_version_dim_table',
    dag=dag
)
load_article_version_dimension_table.doc_md = """
#Dummy operator
"""

load_article_categories_dimension_table = RedshiftExecuteSQLOperator(
    task_id='load_article_categories_dim_table',
    dag=dag
)
load_article_categories_dimension_table.doc_md = """
#Dummy operator
"""

load_article_authors_dimension_table = RedshiftExecuteSQLOperator(
    task_id='load_authors_dim_table',
    dag=dag
)
load_article_authors_dimension_table.doc_md = """
#Dummy operator
"""

load_article_authors_bridge_table = RedshiftExecuteSQLOperator(
    task_id='load_article_authors_bridge_table',
    dag=dag
)
load_article_authors_bridge_table.doc_md = """
#Dummy operator
"""

load_article_citations_dimension_table = RedshiftExecuteSQLOperator(
    task_id='load_article_citations_dim_table',
    dag=dag
)
load_article_citations_dimension_table.doc_md = """
#Dummy operator
"""

load_article_classifications_dimension_table = RedshiftExecuteSQLOperator(
    task_id='load_article_classifications_dim_table',
    dag=dag
)
load_article_classifications_dimension_table.doc_md = """
#Dummy operator
"""

load_classifications_dimension_table = RedshiftExecuteSQLOperator(
    task_id='load_classifications_dim_table',
    dag=dag
)
load_classifications_dimension_table.doc_md = """
#Dummy operator
"""

run_quality_checks = DataQualityOperator(
    task_id='run_quality_checks',
    dag=dag
)
run_quality_checks.doc_md = """
#Dummy operator
"""

end_operator = DummyOperator(
    task_id='Stop_execution',  
    dag=dag
)
end_operator.doc_md = """
#Dummy operator
"""


# Task Dependencies

start_operator >> create_staging_tables_redshift

create_staging_tables_redshift >> [stage_metadata_to_redshift, 
                                   stage_authors_to_redshift,
                                   stage_citations_to_redshift, 
                                   stage_classifications_to_redshift] >> create_main_tables_redshift

create_main_tables_redshift >> load_articles_table

load_articles_table >> [load_article_version_dimension_table, 
                        load_article_categories_dimension_table, 
                        load_article_authors_dimension_table,
                        load_article_authors_bridge_table,
                        load_article_citations_dimension_table,
                        load_article_classifications_dimension_table,
                        load_classifications_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator
    
# End