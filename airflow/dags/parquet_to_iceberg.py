from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    TimestampType,
    DoubleType,
    StringType,
    StructType
)
import boto3
import tempfile
import pandas as pd

def ensure_bucket_exists(s3_client, bucket_name):
    """Create bucket if it doesn't exist"""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except:
        try:
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"Created bucket: {bucket_name}")
        except Exception as e:
            print(f"Error creating bucket {bucket_name}: {e}")

def load_parquet_to_iceberg(**context):
    """Convert parquet files from bronze to Iceberg tables"""
    # Initialize MinIO client
    s3 = boto3.client('s3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin'
    )
    
    # Ensure required buckets exist
    ensure_bucket_exists(s3, 'warehouse')
    
    # Initialize Iceberg catalog
    catalog = load_catalog(
        'hive',
        **{
            'type': 'hive',
            'uri': 'http://minio:9000',
            'warehouse': 'warehouse',
            's3.endpoint': 'http://minio:9000',
            's3.access-key-id': 'minioadmin',
            's3.secret-access-key': 'minioadmin',
            's3.path-style-access': 'true'
        }
    )
    
    # Define schema for measurements
    measurement_schema = Schema(
        StructType({
            'timestamp': TimestampType(),
            'value': DoubleType(),
            'measurement_type': StringType(),
            'machine_id': StringType(),
            'source_file': StringType()
        })
    )
    
    try:
        # Create namespace if it doesn't exist
        if 'default' not in catalog.list_namespaces():
            catalog.create_namespace('default')
        
        # Create or get Iceberg table
        table_name = 'default.measurements'
        try:
            table = catalog.load_table(table_name)
            print(f"Using existing table: {table_name}")
        except:
            table = catalog.create_table(
                identifier=table_name,
                schema=measurement_schema,
                location='warehouse/measurements'
            )
            print(f"Created new table: {table_name}")
        
        # List parquet files in bronze bucket
        response = s3.list_objects_v2(Bucket='bronze')
        
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].endswith('.parquet'):
                    print(f"Processing: {obj['Key']}")
                    
                    # Download parquet file
                    with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp:
                        s3.download_file(
                            Bucket='bronze',
                            Key=obj['Key'],
                            Filename=tmp.name
                        )
                        
                        # Read parquet into pandas
                        df = pd.read_parquet(tmp.name)
                        
                        # Add metadata if missing
                        if 'machine_id' not in df.columns:
                            df['machine_id'] = 'machine_2'
                        if 'source_file' not in df.columns:
                            df['source_file'] = obj['Key']
                        
                        # Write to Iceberg table
                        table.append(df)
                        print(f"Successfully loaded {obj['Key']} to Iceberg")
        
        # Print summary
        print("\nIceberg table summary:")
        print(f"Location: {table.location()}")
        print(f"Schema: {table.schema()}")
        
    except Exception as e:
        print(f"Error in Iceberg conversion: {str(e)}")
        raise e

with DAG(
    'parquet_to_iceberg',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['transform']
) as dag:

    convert_task = PythonOperator(
        task_id='convert_to_iceberg',
        python_callable=load_parquet_to_iceberg
    )