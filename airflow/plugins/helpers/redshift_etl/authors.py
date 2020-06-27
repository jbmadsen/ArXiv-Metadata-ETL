import os
import logging
import json
import pandas as pd
from airflow import AirflowException
from airflow.hooks.S3_hook import S3Hook
#from airflow.hooks.postgres_hook import PostgresHook
#from airflow.contrib.hooks.aws_hook import AwsHook


def load_authors(aws_credentials_id, redshift_connection_id, s3_credentials_id, region, bucket, file_name, **kwargs):
    """
    Downloads file from S3, parses and re-saves it to a format more suited for COPY to Redshift, 
    then re-uploads file back to S3

    Args:
        aws_credentials_id (string): Airflow connection string to connect to AWS
        redshift_connection_id (string): Airflow connection string to connect to Redshift (unused currently)
        s3_credentials_id (string): Airflow connection string to connect to S3
        region (string): Specifies the AWS region you are connecting to
        bucket (string): Name of the bucket of the file
        file_name (string): Name of the file within the bucket

    Raises:
        AirflowException: Throws exception if no file can be found matching the input parameters

    Returns:
        boolean: Returns True if the function ran successfully
    """

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

    logging.info("Done")
    return True
