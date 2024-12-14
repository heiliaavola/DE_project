import duckdb
import boto3
import os
import tempfile
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import IntegerType, StringType
import pyarrow as pa
import pyarrow.parquet as pq


def parquet_to_iceberg(**context):
    """Transform Parquet files from Bronze bucket to Iceberg and upload to Silver bucket."""
    # Initialize MinIO client
    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        verify=False
    )

    # Create silver bucket if it doesn't exist
    try:
        s3.head_bucket(Bucket='silver')
    except:
        s3.create_bucket(Bucket='silver')

    # List Parquet files in the bronze bucket
    response = s3.list_objects_v2(Bucket='bronze')
    parquet_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.parquet')]

    # Load Iceberg catalog
    try:
        catalog = load_catalog(name="rest")
    except Exception as e:
        print(f"Problems with catalog: {e}")

    namespace = "default"
  
    print("Namespace created")
    try:
        catalog.create_namespace(namespace)
    except Exception as e:
        print(f"Namespace '{namespace}' already exists: {e}")

    # Process each Parquet file
    for parquet_file in parquet_files:
        print(f"Processing {parquet_file}")

        try:
            # Download Parquet file from bronze bucket
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_parquet:
                s3.download_fileobj('bronze', parquet_file, tmp_parquet)
                tmp_parquet_path = tmp_parquet.name
            
            # Create Iceberg table using DuckDB
            iceberg_table_dir = f"silver/{parquet_file.replace('.parquet', '')}_iceberg"
            iceberg_table_path = f"s3://warehouse/{iceberg_table_dir}" #ANETT: MA ARVAN, ET SIIN SEE warehouse ÜLELIIGNE, AGA EI KONTROLLINUD
            
            # Read the Parquet data using DuckDB
            with duckdb.connect() as conn:
                # Install and load required extensions
                conn.execute("INSTALL httpfs")
                conn.execute("LOAD httpfs")
                
                # Configure S3 settings
                conn.sql("""
                SET s3_region='us-east-1'; 
                SET s3_url_style='path';
                SET s3_endpoint='minio:9000';
                SET s3_access_key_id='minioadmin';
                SET s3_secret_access_key='minioadmin';
                SET s3_use_ssl=false;
                """)

                # Read Parquet data directly into Arrow format
                print(f"Reading Parquet file: {tmp_parquet_path}")
                conn.sql(f"CREATE TABLE tmp AS SELECT * FROM read_parquet('{tmp_parquet_path}')")
                print("select worked")
                arrow_table = conn.sql(f"SELECT * FROM tmp").arrow()
                print("arrow worked")
                schema = arrow_table.schema #Get Iceberg Schema 
                print("schema worked")
            
                
            # Create table name from file path (removing directory structure and extension)
            table_name = f"{os.path.basename(parquet_file).replace('.parquet', '')}_table"
            print("table_name worked: " + table_name)

            # Create or replace Iceberg table
            full_table_name = f"{namespace}.{table_name}"
            print("full_table_name worked: " + full_table_name)
                
            # Proceed with table creation anyway
            table = catalog.create_table(
                identifier=full_table_name,
                schema=schema,
                location=iceberg_table_path,
            )
            print("table created worked")
            
            table.append(arrow_table)

            # Clean up temporary Parquet file
            os.remove(tmp_parquet_path)
            print(f"Successfully converted {parquet_file} to Iceberg format.")

        except Exception as e:
            print(f"Error processing {parquet_file}: {str(e)}")
            raise  # Re-raise the exception to mark the task as failed

    # Verify silver bucket contents
    try:
        response = s3.list_objects_v2(Bucket='silver')
        print("\nContents of silver bucket:")
        for obj in response.get('Contents', []):
            print(f"- {obj['Key']} (Size: {obj['Size']} bytes)")
    except Exception as e:
        print(f"Error listing silver bucket contents: {str(e)}")
        raise


with DAG(
    'parquet_to_iceberg',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['transform']
) as dag:

    convert_task = PythonOperator(
        task_id='parquet_to_iceberg',
        python_callable=parquet_to_iceberg
    )
    