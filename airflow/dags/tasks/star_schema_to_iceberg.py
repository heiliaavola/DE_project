import duckdb
import boto3
import os
import tempfile
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyiceberg.catalog import load_catalog
import pyarrow as pa


def star_schema_to_iceberg(**context):
    """Load star schema tables from DuckDB to MinIO gold bucket."""
    # Initialize MinIO client
    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        verify=False
    )

    # Create gold bucket if it doesn't exist
    try:
        s3.head_bucket(Bucket='gold')
    except:
        s3.create_bucket(Bucket='gold')

    # Load Iceberg catalog
    try:
        catalog = load_catalog(name="rest")
        print("catalog is fine")
    except Exception as e:
        print(f"Problems with catalog: {e}")

    namespace = "default"
    print("Namespace created")
    try:
        catalog.create_namespace(namespace)
    except Exception as e:
        print(f"Namespace '{namespace}' already exists: {e}") 

    # Connect to DuckDB and export star schema tables
    duckdb_path = "/app/data/warehouse.duckdb"
    with duckdb.connect(duckdb_path) as conn:
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

        # Get the list of star schema tables
        star_schema_tables = [
            "dim_machine",
            "dim_measurement",
            "dim_date",
            "dim_time",
            "dim_operator",
            "dim_electrode_material",
            "dim_dat_metadata",
            "fact_ndax_measurements",
            "fact_dat_measurements"
        ]

        for table_name in star_schema_tables:
            print(f"Processing table: {table_name}")

            try:
                # Read table data into Arrow format
                arrow_table = conn.sql(f"SELECT * FROM {table_name}").arrow()
                print(f"Read table {table_name} into Arrow format.")

                # Create Iceberg table path in the gold bucket
                iceberg_table_dir = f"gold/{table_name}_iceberg"
                iceberg_table_path = f"s3://warehouse/{iceberg_table_dir}"

                # Create or replace Iceberg table
                full_table_name = f"{namespace}.{table_name}"
                print(f"Creating Iceberg table: {full_table_name}")

                # Check if the table already exists
                try:
                    existing_table = catalog.load_table(full_table_name)
                    print(f"Table {full_table_name} already exists. Appending data...")
                except Exception:
                    # Table does not exist, so create it
                    schema = arrow_table.schema
                    catalog.create_table(
                        identifier=full_table_name,
                        schema=schema,
                        location=iceberg_table_path,
                    )
                    print(f"Created Iceberg table: {full_table_name}")

                # Append data to the Iceberg table
                table = catalog.load_table(full_table_name)
                table.append(arrow_table)
                print(f"Appended data to Iceberg table: {full_table_name}")
                #schema = arrow_table.schema
                #table = catalog.create_table(
                #    identifier=full_table_name,
                #    schema=schema,
                #    location=iceberg_table_path,
                #)
                #print(f"Created Iceberg table: {full_table_name}")

                # Append data to the Iceberg table
                #table.append(arrow_table)
                #print(f"Appended data to Iceberg table: {full_table_name}")

            except Exception as e:
                print(f"Error processing table {table_name}: {str(e)}")
                raise  # Re-raise the exception to mark the task as failed

    # Verify gold bucket contents
    try:
        response = s3.list_objects_v2(Bucket='gold')
        print("\nContents of gold bucket:")
        for obj in response.get('Contents', []):
            print(f"- {obj['Key']} (Size: {obj['Size']} bytes)")
    except Exception as e:
        print(f"Error listing gold bucket contents: {str(e)}")
        raise

"""
with DAG(
    'star_schema_to_iceberg',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['load']
) as dag:

    load_task = PythonOperator(
        task_id='star_schema_to_iceberg',
        python_callable=star_schema_to_iceberg
    )
"""

def get_star_schema_to_iceberg_task(dag):
    return PythonOperator(
        task_id='star_schema_to_iceberg',
        python_callable=star_schema_to_iceberg,
        dag=dag
    )