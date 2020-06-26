# ArXiv-Metadata-ETL

![ArXiv banner](./assets/arxiv_banner.png "ArXiv.org")

ArXiv Metadata from Kaggle ETL pipeline 
 - A Udacity Data Engineering Capstone Project
 

## Plan of attack

```
Assets
	[All assets needed for README]
Airflow
	[Dags]
	[Plugins]
	docker-compose.yml
	requirements.txt
Data
	[Empty initially]
Exploration
	[Notebooks to explore data in Data folder]
Setup
	[Download script OR README for how to download all needed data from Kaggle + ArXiv website]
	[Script for loading all data to S3]
	[Script for adding variables/connections/users to Airflow]
	[misc setup scripts needed]
config.cfg
README.md
```

```
TODO:
	Load data from sources to disk
		Source 1: Kaggle (download .json files)
		Source 2: ArXiv homepage (scrape using Python to .csv files)
	Load data from disk to S3
	Run empty airflow
	Add connections/variables/users to airflow
	Load and transform data from S3 into Redshift/Postgres using Airflow
	Create quality checks for staged and transformed data
	Deleted/moved unneeded files for the project
	Updated doc-strings all around
	Updated this README to reflect project files, setup and execution
```
