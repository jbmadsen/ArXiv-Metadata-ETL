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


def upload_file(s3_client, bucket_name, path, folder_name, file_name):
    """Uploads file to S3 bucket

    Args:
        s3_client (boto3 S3 client): A boto3 S3 client object
        bucket_name (str): S3 bucketname to upload file to
        path (str): Path of file to upload
        folder_name (str): Folder name of file to upload
        file_name (str): Filename of file to upload

    Returns:
        (object): Upload response from boto3 S3 client
    """
    try:
        # https://stackoverflow.com/questions/41827963/track-download-progress-of-s3-file-using-boto3-and-callbacks
        full_name = os.path.join(path, folder_name, file_name)
        print("Uploading:", full_name)
        s3_path = f'staging/{folder_name}/{file_name}'
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
    except Exception as e:
        print(f"Error: {e}")
        return None


if __name__ == "__main__":
    # Configurations
    bucket_name = 'arxiv-etl'
    arxiv_src = '../data/arxiv.zip'
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

    # Move files to individual folders
    src = '../data/loading/arxiv-metadata-oai-snapshot.json'
    dst = '../data/loading/metadata/arxiv-metadata-oai-snapshot.json'
    if os.path.exists(src):
        os.makedirs(os.path.dirname('../data/loading/metadata/'), exist_ok=True)
        shutil.move(src, dst)

    src = '../data/loading/authors-parsed.json'
    dst = '../data/loading/authors/authors-parsed.json'
    if os.path.exists(src):
        os.makedirs(os.path.dirname('../data/loading/authors/'), exist_ok=True)
        shutil.move(src, dst)

    src = '../data/loading/internal-citations.json'
    dst = '../data/loading/citations/internal-citations.json'
    if os.path.exists(src):
        os.makedirs(os.path.dirname('../data/loading/citations/'), exist_ok=True)
        shutil.move(src, dst)

    src = '../data/subject-classifications.csv'
    dst = '../data/loading/classifications/subject-classifications.csv'
    if os.path.exists(src):
        os.makedirs(os.path.dirname('../data/loading/classifications/'), exist_ok=True)
        shutil.move(src, dst)

    # Sync data/loaded folder to s3
    # https://dev.to/razcodes/how-to-copy-files-to-s3-using-boto3-41fp
    directory = os.fsencode(data_folder_name)

    print("Starting upload to S3")
    for folder in os.listdir(directory):
        folder_name = os.fsdecode(folder)
        print("Path:", folder_name)
        dir = os.fsencode(os.path.join(data_folder_name, folder_name))
        for file in os.listdir(dir):
            file_name = os.fsdecode(file)
            if file_name.endswith(".json") or file_name.endswith(".csv"): 
                try:
                    response = upload_file(s3_client, bucket_name, data_folder_name, folder_name, file_name)
                    if response is not None:
                        print("HTTPStatusCode:", response['ResponseMetadata']['HTTPStatusCode'])
                except Exception as e:
                    print(f"Error: {e}")
    
    # Delete data/loaded folder 
    print("Deleting temp folder")
    shutil.rmtree(data_folder_name)

    print("Done")