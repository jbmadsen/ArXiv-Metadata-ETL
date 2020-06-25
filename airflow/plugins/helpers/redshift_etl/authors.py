import os
import logging
import json
import pandas as pd
from airflow import AirflowException
from airflow.hooks.S3_hook import S3Hook
#from airflow.hooks.postgres_hook import PostgresHook
#from airflow.contrib.hooks.aws_hook import AwsHook


def load_authors(aws_credentials_id, redshift_connection_id, s3_credentials_id, region, bucket, file_name, **kwargs):

    logging.info("Getting S3 hook")
    s3 = S3Hook(s3_credentials_id)
    
    logging.info(f"Downloading {file_name} from bucket {bucket}")
    data_file = s3.read_key(bucket_name=bucket, key=file_name)

    if not data_file:
        error = "No data downloaded from S3"
        logging.info(error)    
        raise AirflowException(error)

    logging.info("Converting to JSON")
    json_file = json.loads(data_file)

    logging.info("Parsing raw JSON")
    authors = []

    for key in json_file:
        value = json_file[key]
        #print("{} : {}".format(key, value))
        for author_list in value:
            author = {"metadata_id": key, "author": f"{author_list[1].replace(',', '')} {author_list[0].replace(',', '')} {author_list[2].replace(',', '')}"}
            #print(author)
            authors.append(author)

    logging.info("Loading JSON into Dataframe")
    authors_df = pd.DataFrame(authors)
    
    logging.info("Saving DataFrame to disk")
    save_file_name = 'authors_parsed.csv'

    if os.path.exists(file_name):
        # Delete before saving if exists
        os.remove(save_file_name)
    
    authors_df.to_csv(save_file_name, index=False)

    logging.info("Copying file to S3")
    s3_file_name = f"staging/authors/{save_file_name}"
    s3.load_file(save_file_name, s3_file_name, bucket_name=bucket, replace=True)

    logging.info("Deleting DataFrame from disk")
    os.remove(save_file_name)

    # logging.info("Getting AWS hooks")
    # aws_hook = AwsHook(aws_credentials_id)

    # logging.info("Loading AWS credentials")
    # credentials = aws_hook.get_credentials()

    # logging.info("Getting Redshift hook")
    # redshift = PostgresHook(postgres_conn_id=redshift_connection_id)

    # logging.info("Creating Redshift query")
    # copy_query= """
    #     COPY public.staging_authors
    #     FROM '{}'
    #     ACCESS_KEY_ID '{}'
    #     SECRET_ACCESS_KEY '{}'
    #     REGION '{}'
    #     DELIMITER ','
    #     CSV;
    # """.format(
    #     f"s3://{bucket}/{s3_file_name}",
    #     credentials.access_key,
    #     credentials.secret_key,
    #     region
    # )   

    # logging.info("Executing Redshift query: Copying from S3")
    # redshift.run(copy_query)

    logging.info("Done")
    return True
