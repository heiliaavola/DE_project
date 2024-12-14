import duckdb
import boto3
import os
import tempfile
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyiceberg.catalog import load_catalog
import pyarrow as pa


def process_machine_1_file(parquet_file, s3, catalog, namespace):
    """Process Parquet file from machine_1."""
    print(f"Processing machine_1 file: {parquet_file}")

    try:
        # Download Parquet file
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_parquet:
            s3.download_fileobj('bronze', parquet_file, tmp_parquet)
            tmp_parquet_path = tmp_parquet.name

        # Create Iceberg table path
        iceberg_table_dir = f"silver/{parquet_file.replace('.parquet', '')}_iceberg"
        iceberg_table_path = f"s3://warehouse/{iceberg_table_dir}"

        # Read the Parquet data using DuckDB
        with duckdb.connect() as conn:
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

            # Exclude problematic column and adjust timestamp
            conn.sql(f"""
                CREATE TABLE tmp AS 
                SELECT 
                    record_ID,
                    cycle,
                    step_ID,
                    step_name,
                    time_in_step,
                    voltage_V,
                    current_mA,
                    capacity_mAh,
                    energy_mWh,
                    CAST(timestamp AS TIMESTAMP) AS adjusted_timestamp, -- Handle TIMESTAMP_NS
                    Validated
                FROM read_parquet('{tmp_parquet_path}')
            """)
            print("Created temporary table for machine_1.")

            # Read table into Arrow format
            arrow_table = conn.sql(f"SELECT * FROM tmp").arrow()
            print(f"Read table {parquet_file} into Arrow format.")

        # Create table name
        table_name = f"{os.path.basename(parquet_file).replace('.parquet', '')}_table"
        full_table_name = f"{namespace}.{table_name}"

        # Create Iceberg table
        table = catalog.create_table(
            identifier=full_table_name,
            schema=arrow_table.schema,
            location=iceberg_table_path,
        )
        table.append(arrow_table)

        os.remove(tmp_parquet_path)
        print(f"Successfully processed machine_1 file: {parquet_file}")

    except Exception as e:
        print(f"Error processing machine_1 file {parquet_file}: {str(e)}")
        raise


def process_machine_2_file(parquet_file, s3, catalog, namespace):
    """Process Parquet file from machine_2."""
    print(f"Processing machine_2 file: {parquet_file}")

    try:
        # Download Parquet file
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_parquet:
            s3.download_fileobj('bronze', parquet_file, tmp_parquet)
            tmp_parquet_path = tmp_parquet.name

        # Create Iceberg table path
        iceberg_table_dir = f"silver/{parquet_file.replace('.parquet', '')}_iceberg"
        iceberg_table_path = f"s3://warehouse/{iceberg_table_dir}"

        # Read the Parquet data using DuckDB
        with duckdb.connect() as conn:
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

            # Read Parquet data without timestamp adjustment
            conn.sql(f"CREATE TABLE tmp AS SELECT * FROM read_parquet('{tmp_parquet_path}')")
            arrow_table = conn.sql(f"SELECT * FROM tmp").arrow()

        # Create table name
        table_name = f"{os.path.basename(parquet_file).replace('.parquet', '')}_table"
        full_table_name = f"{namespace}.{table_name}"

        # Create Iceberg table
        table = catalog.create_table(
            identifier=full_table_name,
            schema=arrow_table.schema,
            location=iceberg_table_path,
        )
        table.append(arrow_table)

        os.remove(tmp_parquet_path)
        print(f"Successfully processed machine_2 file: {parquet_file}")

    except Exception as e:
        print(f"Error processing machine_2 file {parquet_file}: {str(e)}")
        raise

def process_metadata_file(parquet_file, s3, catalog, namespace):
    """Process metadata Parquet file."""
    print(f"Processing metadata file: {parquet_file}")

    try:
        # Download Parquet file
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_parquet:
            s3.download_fileobj('bronze', parquet_file, tmp_parquet)
            tmp_parquet_path = tmp_parquet.name

        # Create Iceberg table path
        iceberg_table_dir = f"silver/{parquet_file.replace('.parquet', '')}_metadata"
        iceberg_table_path = f"s3://warehouse/{iceberg_table_dir}"

        # Read the Parquet data using DuckDB
        with duckdb.connect() as conn:
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

            # Read metadata Parquet file
            conn.sql(f"CREATE TABLE tmp AS SELECT * FROM read_parquet('{tmp_parquet_path}')")
            arrow_table = conn.sql(f"SELECT * FROM tmp").arrow()

        # Create table name
        table_name = f"{os.path.basename(parquet_file).replace('.parquet', '')}_table"
        full_table_name = f"{namespace}.{table_name}"

        # Create Iceberg table
        table = catalog.create_table(
            identifier=full_table_name,
            schema=arrow_table.schema,
            location=iceberg_table_path,
        )
        table.append(arrow_table)

        os.remove(tmp_parquet_path)
        print(f"Successfully processed metadata file: {parquet_file}")

    except Exception as e:
        print(f"Error processing metadata file {parquet_file}: {str(e)}")
        raise


def parquet_to_iceberg(**context):
    """Process Parquet files from bronze bucket."""
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
    catalog = load_catalog(name="rest")
    namespace = "default"

    # Create namespace if not exists
    try:
        catalog.create_namespace(namespace)
    except Exception as e:
        print(f"Namespace '{namespace}' already exists: {e}")

    # Process files
    for parquet_file in parquet_files:
        if "machine_1" in parquet_file and "metadata" not in parquet_file:
            process_machine_1_file(parquet_file, s3, catalog, namespace)
        elif "machine_2" in parquet_file and "metadata" not in parquet_file:
            process_machine_2_file(parquet_file, s3, catalog, namespace)
        elif "metadata" in parquet_file:
            process_metadata_file(parquet_file, s3, catalog, namespace)
        else:
            print(f"Skipping unrecognized file: {parquet_file}")

"""
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
"""

def get_parquet_to_iceberg_task(dag):
    return PythonOperator(
        task_id='parquet_to_iceberg',
        python_callable=parquet_to_iceberg,
        dag=dag
    )
