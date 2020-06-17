# Imports
import os
import boto3
import zipfile
import shutil
import progressbar


def connect(region = 'us-east-1'):
    """Creates a boto3 S3 client

    Args:
        region (str, optional): The AWS region to connect to. Defaults to 'us-east-1'.
    
    Returns:
        (boto3 client object): a boto3 S3 client
    """
    # Create connections
    s3_client = boto3.client('s3', region_name=region)
    return s3_client


def create_bucket(s3_client, bucket_name = 'arxiv-etl'):
    """Creates an S3 bucket if one does not already exists

    Args:
        s3_client (boto3 client object): A boto3 S3 client
        bucket_name (str, optional): The bucket name to create. Defaults to 'arxiv-etl'.
    """
    # Retrieve the list of existing buckets
    response = s3_client.list_buckets()

    # Check if bucket already exists
    bucket_exists = False
    for obj in response['Buckets']:
        if obj['Name'] == bucket_name:
            return
    
    # Create bucket if it doesn't exist
    if not bucket_exists:
        s3_client.create_bucket(Bucket=bucket_name)


def upload_file(s3_client, folder_name, file_name, bucket_name, bucket_folder = 'staging'):
    """Uploads file to S3 bucket

    Args:
        s3_client (boto3 S3 client): A boto3 S3 client object
        folder_name (str): Folder path of file to upload
        file_name (str): Filename of file to upload
        bucket_name (str): S3 bucketname to upload file to
        bucket_folder (str): S3 folder name to upload file to. Defaults to 'staging'.

    Returns:
        (object): Upload response from boto3 S3 client
    """
    # https://stackoverflow.com/questions/41827963/track-download-progress-of-s3-file-using-boto3-and-callbacks
    full_name = os.path.join(folder_name, file_name)
    s3_path = f'{bucket_folder}/{file_name}'
    # Create progress info
    statinfo = os.stat(full_name)
    up_progress = progressbar.progressbar.ProgressBar(maxval=statinfo.st_size)
    up_progress.start()
    # Helper function for progress display
    def upload_progress(chunk):
        up_progress.update(up_progress.currval + chunk)

    # Upload to S3
    response = s3_client.upload_file(full_name, bucket_name, s3_path, Callback=upload_progress)
    # Done
    up_progress.finish()
    return response


if __name__ == "__main__":
    # Configurations
    bucket_name = 'arxiv-etl'
    arxiv_src = '../data/arxiv.zip'
    classification_src = '../data/subject-classifications.csv'
    data_folder_name = '../data/loading/'
    
    # Connect and create bucket
    print(f"Connecting to S3 and creating bucket: {bucket_name}")
    s3_client = connect()
    create_bucket(s3_client, bucket_name)

    #Unzip files to new folder
    # https://stackoverflow.com/questions/3451111/unzipping-files-in-python
    print(f"Extracting data")
    with zipfile.ZipFile(arxiv_src, 'r') as zip_ref:
        zip_ref.extractall(data_folder_name)
    
    print("Copying data")
    # Copy the classification data to loading folder to group it, which lets us sync the folder, and we can delete it after to save space
    classification_dst = '../data/loading/subject-classifications.csv'
    shutil.copyfile(classification_src, classification_dst)

    # Sync data/loaded folder to s3
    # https://dev.to/razcodes/how-to-copy-files-to-s3-using-boto3-41fp
    directory = os.fsencode(data_folder_name)

    print("Starting upload to S3")
    for file in os.listdir(directory):
        file_name = os.fsdecode(file)
        if file_name.endswith(".json") or file_name.endswith(".csv"): 
            print("Uploading", file_name)
            response = upload_file(s3_client, data_folder_name, file_name, bucket_name)
            if response is not None:
                print("HTTPStatusCode:", response['ResponseMetadata']['HTTPStatusCode'])
    
    # Delete data/loaded folder 
    print("Deleting temp folder")
    shutil.rmtree(data_folder_name)

    print("Done")