from datetime import datetime
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
import os

def upload_raw_data(**context):
    """Upload both DAT and XLSX files to MinIO"""
    s3 = boto3.client('s3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        config=boto3.session.Config(s3={'addressing_style': 'path'})
    )
    
    # Ensure bucket exists
    try:
        s3.head_bucket(Bucket='raw_data')
    except:
        s3.create_bucket(Bucket='raw_data')
    
    # Define paths
    BASE_DIR = '/opt/airflow/project_data/airflow/project_data/anonymized_data_package'
    MACHINE1_DIR = f"{BASE_DIR}/machine_1"
    MACHINE2_DIR = f"{BASE_DIR}/machine_2"
    
    uploaded_files = []
    
    # Upload DAT files from machine_2
    for filename in sorted(os.listdir(MACHINE2_DIR)):
        if filename.endswith('.dat'):
            file_path = os.path.join(MACHINE2_DIR, filename)
            try:
                with open(file_path, 'rb') as file_obj:
                    s3.put_object(
                        Bucket='raw_data',
                        Key=f'machine_2/{filename}',
                        Body=file_obj
                    )
                uploaded_files.append(f'machine_2/{filename}')
                print(f"Successfully uploaded {filename}")
            except Exception as e:
                print(f"Error uploading {filename}: {str(e)}")
    
    # Upload XLSX files from machine_1
    for filename in sorted(os.listdir(MACHINE1_DIR)):
        if filename.endswith('.xlsx'):
            file_path = os.path.join(MACHINE1_DIR, filename)
            try:
                with open(file_path, 'rb') as file_obj:
                    s3.put_object(
                        Bucket='raw_data',
                        Key=f'machine_1/{filename}',
                        Body=file_obj
                    )
                uploaded_files.append(f'machine_1/{filename}')
                print(f"Successfully uploaded {filename}")
            except Exception as e:
                print(f"Error uploading {filename}: {str(e)}")
    
    # Verify uploads
    try:
        response = s3.list_objects_v2(Bucket='raw_data')
        print("\nContents of raw_data bucket:")
        for obj in response.get('Contents', []):
            print(f"- {obj['Key']} (Size: {obj['Size']} bytes)")
    except Exception as e:
        print(f"Error listing bucket contents: {str(e)}")
    
    return uploaded_files

"""
with DAG(
    'upload_raw_files',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['raw']
) as dag:

    upload_task = PythonOperator(
        task_id='upload_raw_files',
        python_callable=upload_raw_data
    )
"""

def get_upload_task(dag):
    return PythonOperator(
        task_id='upload_raw_files',
        python_callable=upload_raw_data,
        dag=dag
    )