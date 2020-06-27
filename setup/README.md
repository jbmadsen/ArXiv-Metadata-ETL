# Setting up

### Setup Configurations 

1) Make sure you have all Python requirements from ```requirements.txt``` installed
    * Otherwise run ```pip install requirements.txt``` from the this folder using your favorite terminal to install requirements
2) Make sure you have docker with docker-compose installed locally
    * Run ```docker-compose up -d``` from the airflow folder using your favorite terminal to start up airflow for this project
3) Fill your AWS credentials into ```aws.cfg```
4) Load connections into Airflow using Powershell:
    * Run ```.\add_airflow_connections.ps1``` from the this folder using Powershell
    * Alternatively, if you are on OSX/Linux, it should be trivial to convert the powershell script to a bash script
5) Download the datasets to the /data/ folder:
    * Make sure you have the requirements installed
    * In order to download this via the script, you'll need a [Kaggle](https://www.kaggle.com/) user and have downloaded your kaggle credentials and token to your ~/.kaggle/ folder on your local computer.
    * Run ```python download_datasets.py``` from the this folder using your favorite terminal
6) Load data from /data/ to S3 - this will create a bucket called arxiv-etl is none exists:
    * Run ```python load_to_s3.py``` from the this folder using your favorite terminal
7) Create and launch a Redshift cluster:
    * Run ```python create_redshift_cluster.py``` from the this folder using your favorite terminal

Everything should now be setup, and ready to run the ETL process.
