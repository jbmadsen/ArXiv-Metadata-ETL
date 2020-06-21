import os
import sys
import configparser
import boto3
from botocore.exceptions import ClientError


# Set path to current directory
os.chdir(os.path.dirname(sys.argv[0]))


# Open and read the contents of the config file
ioc_config = configparser.ConfigParser()
ioc_config.read_file(open('./aws.cfg'))


# Load all the keys needed to create AWS services
KEY                    = ioc_config.get('AWS','KEY')
SECRET                 = ioc_config.get('AWS','SECRET')

REGION                 = ioc_config.get("EMR","REGION")
NODE_TYPE              = ioc_config.get("EMR","NODE_TYPE")
NUM_SLAVES             = ioc_config.get("EMR","NUM_SLAVES")
LOGS_PATH              = ioc_config.get("EMR","LOGS_PATH")
JOBS_PATH              = ioc_config.get("EMR","JOBS_PATH")


def terminate_emr_clusters():
    """
    Terminates existing EMR clusters
    """

    # Deleting job files from an S3 bucket
    s3 = boto3.resource('s3',
                        region_name=REGION, 
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,)

    #s3.Object(JOBS_PATH, "emr_jobs").delete()
    
    print("PyStark ETL job Files deleted.")

    # Creating resources/clients for infrastructure: EMR
    emr = boto3.client('emr',
                       region_name=REGION, 
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET,)

    # Get existing clusters
    clusters = emr.list_clusters()

    if not clusters or not clusters['Clusters']:
        print("No clusters exists. Creating one.")
        return
    else:
        for cluster in clusters['Clusters']:
            print("Cluster exists:", "Id:", cluster['Id'], "Name:", cluster['Name'])
            response = emr.terminate_job_flows(JobFlowIds=[cluster['Id'],])
            print("Terminating:", response)


if __name__ == "__main__":
    # Terminate EMR clusters
    terminate_emr_clusters()
