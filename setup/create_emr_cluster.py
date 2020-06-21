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


def create_emr_cluster():
    """
    Creates EMR cluster, if no active cluster is running
    """

    # Creating resources/clients for infrastructure: EMR
    emr = boto3.client('emr',
                       region_name=REGION, 
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET,)

    # Get existing clusters
    clusters = emr.list_clusters()

    if clusters and clusters['Clusters']:
        for cluster in clusters['Clusters']:
            if (cluster['Status']['State'] not in ['TERMINATED', 'TERMINATED_WITH_ERRORS']):
                print("Cluster exists:", cluster)
                return
        print("All found Clusters are terminated. We create a new one.")
    else:
        print("No clusters exists. Creating one.")

    # Creating cluster settings: InstanceGroups
    instance_groups = [
        {
            'Name': "Master",
            'Market': 'SPOT',
            'InstanceRole': 'MASTER',
            'InstanceType': NODE_TYPE,
            'InstanceCount': 1,
        },
        {
            'Name': "Slave",
            'Market': 'SPOT',
            'InstanceRole': 'CORE',
            'InstanceType': NODE_TYPE,
            'InstanceCount': int(NUM_SLAVES),
        }
    ]

    try:
        cluster = None
        # Creating cluster
        cluster = emr.run_job_flow(
            Name='emr_cluster',
            LogUri=LOGS_PATH,
            ReleaseLabel='emr-5.20.0',
            Applications=[
                {'Name': 'Spark'},
            ],
            Instances={
                'InstanceGroups': instance_groups,
                #'Ec2KeyName': '', # Not needed for how this process
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                #'Ec2SubnetId': 'subnet-id', # Not needed for how this process
            },
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
        )
    except ClientError as ex:
        print("ClientError:", ex)
    except ConnectionRefusedError as ex:
        print("ConnectionRefusedError:", ex)
    except Exception as ex:
        print("Exception:", ex) 
    else:
        # Everything went well, lets see if we can query the cluster
        clusters = emr.list_clusters()
        if clusters and clusters['Clusters']:
            print("Creating Cluster:", clusters['Clusters'])

    return cluster 


if __name__ == "__main__":
    # Create EMR cluster
    cluster = create_emr_cluster()

    if cluster is not None:
        print("Creating cluster...")
