# DE_Project

## How to Run

Start the containers:
docker compose up -d 

## Accessing Airflow

Access the Airflow web interface at:
http://localhost:8080/ 

Create an Airflow admin user with this command:
docker exec airflow-webserver airflow users create --username airflow --password airflow --firstname first --lastname last --role Admin --email admin@airflow.org 

## DAG Descriptions

- upload_raw_files.py: Uploads data from airflow/project_data/anonymized_data_package folder to MinIO file storage at data/raw-data
- dat_to_parquet.py: Converts machine2 data from .dat files to .parquet files and saves them to MinIO file storage at bronze/
- parquet_to_iceberg.py: Work in progress