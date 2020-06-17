# Load in all required libraries
import os
import sys
import pandas as pd 
import boto3
import botocore.exceptions
import json
import configparser
import time


# Set path to current directory
os.chdir(os.path.dirname(sys.argv[0]))


# Open and read the contents of the config file
ioc_config = configparser.ConfigParser()
ioc_config.read_file(open('./aws.cfg'))


# Load all the keys needed to create AWS services
KEY                    = ioc_config.get('AWS','KEY')
SECRET                 = ioc_config.get('AWS','SECRET')

DWH_REGION             = ioc_config.get("REDSHIFT","REGION")
DWH_CLUSTER_TYPE       = ioc_config.get("REDSHIFT","CLUSTER_TYPE")
DWH_NUM_NODES          = ioc_config.get("REDSHIFT","NUM_NODES")
DWH_NODE_TYPE          = ioc_config.get("REDSHIFT","NODE_TYPE")
DWH_CLUSTER_IDENTIFIER = ioc_config.get("REDSHIFT","CLUSTER_IDENTIFIER")
DWH_IAM_ROLE_NAME      = ioc_config.get("REDSHIFT","IAM_ROLE_NAME")

DWH_DB                 = ioc_config.get("CLUSTER","DB_NAME")
DWH_DB_USER            = ioc_config.get("CLUSTER","DB_USER")
DWH_DB_PASSWORD        = ioc_config.get("CLUSTER","DB_PASSWORD")
DWH_PORT               = ioc_config.get("CLUSTER","DB_PORT")



def create_client(name, func):
    """Creating resources/clients for all needed infrastructure: EC2, S3, IAM, Redshift
    Keyword arguments:
    name -- the name of the AWS service resource/client 
    func -- the boto3 function object (e.g. boto3.resource/boto3.client) 
    """
    print("Creating client for", name)
    return func(name,
                region_name=DWH_REGION,
                aws_access_key_id=KEY,
                aws_secret_access_key=SECRET)


def create_iam_role(iam):
    """Creating IAM role for Redshift, allowing it to use AWS services
    
    Keyword arguments:
    iam -- a boto3.client for IAM
    """
    print("Creating a new IAM Role") 
    try:
        resp = iam.create_role(Path='/',
                               RoleName=DWH_IAM_ROLE_NAME,
                               Description = "Allows Redshift clusters to call AWS services on your behalf.",
                               AssumeRolePolicyDocument=json.dumps({'Statement': [{'Action': 'sts:AssumeRole',
                                                                                   'Effect': 'Allow',
                                                                                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                                                                    'Version': '2012-10-17'}
                                                                  )
                              )
        print("IAM Role created:")
        print(resp)
    except iam.exceptions.EntityAlreadyExistsException:
        print("IAM Role already created")
    except Exception as e:
        print("Error creating IAM Role:", e)

        
def create_arn_role(iam):
    """Attaching policy to role, and return the ARN role 
    
    Keyword arguments:
    iam -- a boto3.client for IAM
    """
    print("Attaching policy to IAM role")
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")['ResponseMetadata']['HTTPStatusCode']
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    #print("ARN role:", roleArn)
    return roleArn


def create_redshift_cluster(redshift, roleArn):
    """Creates Redshift cluster (Warning, this costs money - make sure to use it or delete it again!)
    
    Keyword arguments:
    iam     -- a boto3.client for Redshift
    roleArn -- a role arn for reading from S3
    """
    cluster = redshift.create_cluster(
        #Hardware provisioned
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),

        #Identifiers & Credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,

        #Roles (for s3 access)
        IamRoles=[roleArn]  
    )
    print("Creating Redshift cluster with", DWH_NUM_NODES, "nodes, on", DWH_REGION)
    return cluster


def query_redshift_status(redshift):
    """Query status of the cluster, returns cluster properties once cluster is available
    
    Keyword arguments:
    iam -- a boto3.client for Redshift
    """
    def prettyRedshiftProps(props, limited = True):
        #pd.set_option('display.max_colwidth', -1)
        if limited:
            keysToShow = ["ClusterStatus"]
        else:
            keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
        x = [(k, v) for k,v in props.items() if k in keysToShow]
        return pd.DataFrame(data=x, columns=["Key", "Value"])

    # Print status, sleep if not available, try again
    while True:
        cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        df = prettyRedshiftProps(cluster_props, limited=True)
        print(df.values)
        if cluster_props['ClusterStatus'] == 'available':
            break
        time.sleep(60) # Sleep 60 seconds, and look again, untill cluster becomes available

    # Print full details once cluster is available
    df = prettyRedshiftProps(cluster_props, limited=False)
    print(df)

    # Return cluster properties
    return cluster_props

    
def get_redshift_endpoint_info(redshift, cluster_props):
    """Get endpoint and ARN role for cluster
    
    Keyword arguments:
    iam           -- a boto3.client for Redshift
    cluster_props -- cluster properties for the created Redshift cluster
    """
    redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

    DWH_ENDPOINT = cluster_props['Endpoint']['Address']
    DWH_ROLE_ARN = cluster_props['IamRoles'][0]['IamRoleArn']
    #print("DWH_ENDPOINT:", DWH_ENDPOINT)
    #print("DWH_ROLE_ARN:", DWH_ROLE_ARN)
    return (DWH_ENDPOINT, DWH_ROLE_ARN)


def update_cluster_security_group(ec2, cluster_props):
    """Update cluster security group to allow access through redshift port
    
    Keyword arguments:
    iam           -- a boto3.resource for EC2
    cluster_props -- cluster properties for the created Redshift cluster
    """
    vpc = ec2.Vpc(id=cluster_props['VpcId'])

    # The first Security group should be the default one
    defaultSg = list(vpc.security_groups.all())[0]
    print("Default Security group:", defaultSg)

    # Authorize access
    try:
        defaultSg.authorize_ingress(GroupName=defaultSg.group_name,
                                    CidrIp='0.0.0.0/0',
                                    IpProtocol='TCP',
                                    FromPort=int(DWH_PORT),
                                    ToPort=int(DWH_PORT)
                                   )
        print("Access authorized")
    except botocore.exceptions.ClientError as e:
        print("ClientError:", e)
    except Exception as e:
        print("Error:", e)


def test_connection():
    """Test connection to created Redshift cluster to validate"""
    import psycopg2

    dwh_config = configparser.ConfigParser()
    dwh_config.read_file(open('./aws.cfg'))

    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*dwh_config['CLUSTER'].values()))
        _ = conn.cursor()
        print('Connected to AWS Redshift cluster')
        conn.close()
    except Exception as e:
        print('Error connecting to AWS Redshift cluster:', e)

        
def main():
    """Standing up a Redshift cluster and saves connection information to redshift.cfg"""
    # Creating resources/clients for all needed infrastructure: EC2, IAM, Redshift
    ec2 = create_client('ec2', boto3.resource)
    iam = create_client('iam', boto3.client)
    redshift = create_client('redshift', boto3.client)
    
    # Create needed IAM / ARN roles for Redshift
    create_iam_role(iam)
    arn_role = create_arn_role(iam)
    
    # Create cluster and await its completion
    create_redshift_cluster(redshift, arn_role)
    cluster_props = query_redshift_status(redshift)
    
    # Get endpoint into to allow querying
    info = get_redshift_endpoint_info(redshift, cluster_props)
    print(info)
    # TODO: Save info to aws.cfg
    
    # Update security groups to ACTUALLY allow querying
    update_cluster_security_group(ec2, cluster_props)
    
    # Test connection to see that everything (hopefully) went well
    test_connection()
    
    # End of main
    return


if __name__ == "__main__":
    main()
