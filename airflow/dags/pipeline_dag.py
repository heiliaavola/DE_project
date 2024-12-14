from datetime import datetime
from airflow import DAG

# Import tasks
from tasks.upload_raw_files import get_upload_task
from tasks.excel_metadata_to_json_to_mongodb import get_convert_excel_metadata_to_json_to_mongodb_task
from tasks.excel_to_parquet import get_convert_excel_to_parquet_task
from tasks.dat_to_parquet import get_convert_dat_to_parquet_task
from tasks.mongodb_to_parquet import get_mongodb_to_parquet_task
from tasks.parquet_to_iceberg import get_parquet_to_iceberg_task 

# Create the DAG
with DAG(
    'combined_processing_pipeline',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['processing']
) as dag:

    # Get all tasks
    upload_task = get_upload_task(dag)
    convert_metadata_to_mongodb_task = get_convert_excel_metadata_to_json_to_mongodb_task(dag)
    convert_excel_to_parquet_task = get_convert_excel_to_parquet_task(dag)
    convert_dat_to_parquet_task = get_convert_dat_to_parquet_task(dag)
    convert_mongodb_to_parquet_task = get_mongodb_to_parquet_task(dag)
    parquet_to_iceberg_task = get_parquet_to_iceberg_task(dag)

    # Set up task dependencies
    upload_task >> convert_metadata_to_mongodb_task >> [convert_excel_to_parquet_task, convert_dat_to_parquet_task, convert_mongodb_to_parquet_task] >> parquet_to_iceberg_task