from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
import os
from LimeNDAX import get_records
import tempfile
import shutil
import tempfile

def convert_ndax_to_parquet(**context):
    """Convert NDAX files from MinIO to Parquet"""
    # Initialize MinIO client
    s3 = boto3.client('s3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin'
    )
    
    # Ensure bronze bucket exists
    try:
        s3.head_bucket(Bucket='bronze')
        print("Bronze bucket exists.")
    except:
        s3.create_bucket(Bucket='bronze')
        print("Created bronze bucket.")

    # Create a local temporary directory
    local_tempdata_dir = tempfile.mkdtemp(prefix="temdata_")
    print(f"Local `.\\temdata` directory created at: {local_tempdata_dir}")
    
    try:
        # List NDAX files from machine_1 in MinIO
        folder_structure = ['long_measurements', 'short_measurements']
        for measurement_type in folder_structure:
            response = s3.list_objects_v2(
                Bucket='raw-data',
                Prefix=f'machine_1/{measurement_type}/'
            )
            
            for obj in response.get('Contents', []):
                if obj['Key'].endswith('.ndax'):
                    print(f"Processing {obj['Key']}")
                    
                    # Get NDAX file from MinIO
                    response = s3.get_object(Bucket='raw-data', Key=obj['Key'])
                    local_file_path = os.path.join(local_tempdata_dir, os.path.basename(obj['Key']))
                    
                    # Save the NDAX file locally
                    with open(local_file_path, 'wb') as f:
                        f.write(response['Body'].read())
                    
                    print(f"Downloaded {obj['Key']} to {local_file_path}")
        
        # Process files using get_records
        for local_file in os.listdir(local_tempdata_dir):
            if local_file.endswith('.ndax'):
                local_file_path = os.path.join(local_tempdata_dir, local_file)  # Use temp directory path
                try:
                    df = get_records(local_file_path)
                    print(f"Processed {local_file}: {len(df)} rows")
                    
                    # Save Parquet file locally and upload to MinIO
                    with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp_parquet:
                        df.to_parquet(tmp_parquet.name, index=False)
                        s3.put_object(
                            Bucket='bronze',
                            Key=f"machine_1/{local_file.replace('.ndax', '.parquet')}",
                            Body=open(tmp_parquet.name, 'rb')
                        )
                        print(f"Uploaded {local_file.replace('.ndax', '.parquet')} to bronze bucket")
                except Exception as e:
                    print(f"Error processing {local_file}: {str(e)}")

    finally:
        # Clean up temporary directory
        shutil.rmtree(local_tempdata_dir)
        print(f"Cleaned up temporary directory: {local_tempdata_dir}")

    
    # Verify bronze bucket contents
    try:
        response = s3.list_objects_v2(Bucket='bronze')
        print("\nContents of bronze bucket:")
        for obj in response.get('Contents', []):
            print(f"- {obj['Key']} (Size: {obj['Size']} bytes)")
    except Exception as e:
        print(f"Error listing bronze bucket contents: {str(e)}")


with DAG(
    'ndax_to_parquet',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['transform', 'machine_1']
) as dag:

    convert_task = PythonOperator(
        task_id='convert_ndax_to_parquet',
        python_callable=convert_ndax_to_parquet
    )
