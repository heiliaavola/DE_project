from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
import pandas as pd
from io import StringIO
import os
import tempfile

def extract_metadata_and_data(file_content, start_marker):
    """Extracts metadata and data from file content"""
    metadata = {}
    table_data = []
    
    # Split content into lines
    lines = file_content.split('\n')
    
    # Extract metadata and find start of table
    data_start_index = None
    for i, line in enumerate(lines):
        if line.startswith(start_marker):
            data_start_index = i + 1
            break
        elif ':' in line:
            key, value = line.split(':', 1)
            metadata[key.strip()] = value.strip()
    
    if data_start_index is not None:
        table_data = lines[data_start_index:]
    
    # Combine table data and parse
    data_str = '\n'.join(table_data)
    df = pd.read_csv(StringIO(data_str), sep="\t")
    
    # Add metadata as columns
    for key, value in metadata.items():
        df[key] = value
    
    return metadata, df

def convert_to_parquet(**context):
    """Convert DAT files from MinIO to Parquet"""
    # Initialize MinIO client
    s3 = boto3.client('s3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin'
    )
    
    # Create bronze bucket if it doesn't exist
    try:
        s3.head_bucket(Bucket='bronze')
    except:
        s3.create_bucket(Bucket='bronze')
    
    # Get list of DAT files from raw-data
    response = s3.list_objects_v2(
        Bucket='raw-data',  # Just the bucket name
        Prefix='machine_2/'  # The "directory" path
    )
    
    data_start_marker = "Measurement Data:"
    
    # Process each DAT file
    for obj in response.get('Contents', []):
        if obj['Key'].endswith('.dat'):
            print(f"Processing {obj['Key']}")
            
            try:
                # Get DAT file from MinIO
                response = s3.get_object(Bucket='raw-data', Key=obj['Key'])
                file_content = response['Body'].read().decode('ISO-8859-1')
                
                # Extract data and convert to parquet
                metadata, df = extract_metadata_and_data(file_content, data_start_marker)
                print(f"Processed {obj['Key']}: {len(df)} rows")
                
                # Save to temporary parquet file
                with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp:
                    df.to_parquet(tmp.name, index=False)
                    
                    # Upload to bronze bucket
                    parquet_key = f"{obj['Key'].replace('.dat', '.parquet')}"
                    with open(tmp.name, 'rb') as parquet_file:
                        s3.put_object(
                            Bucket='bronze',
                            Key=parquet_key,
                            Body=parquet_file
                        )
                    print(f"Uploaded {parquet_key} to bronze bucket")
            
            except Exception as e:
                print(f"Error processing {obj['Key']}: {str(e)}")
    
    # Verify bronze bucket contents
    try:
        response = s3.list_objects_v2(Bucket='bronze')
        print("\nContents of bronze bucket:")
        for obj in response.get('Contents', []):
            print(f"- {obj['Key']} (Size: {obj['Size']} bytes)")
    except Exception as e:
        print(f"Error listing bronze bucket contents: {str(e)}")

with DAG(
    'dat_to_parquet',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['transform']
) as dag:

    convert_task = PythonOperator(
        task_id='convert_to_parquet',
        python_callable=convert_to_parquet
    )