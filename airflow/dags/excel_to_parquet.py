from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
import pandas as pd
import tempfile

def convert_to_parquet(**context):
    """Convert Excel files from MinIO to Parquet"""
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
    
    # Get list of Excel files from raw-data
    response = s3.list_objects_v2(
        Bucket='raw-data',
        Prefix='machine_1/'
    )
    
    # Process each Excel file
    for obj in response.get('Contents', []):
        if obj['Key'].endswith(('.xlsx', '.xls')):
            print(f"Processing {obj['Key']}")
            
            try:
                # Get Excel file from MinIO
                response = s3.get_object(Bucket='raw-data', Key=obj['Key'])
                
                # Save the Excel file temporarily
                with tempfile.NamedTemporaryFile(suffix='.xlsx') as tmp_excel:
                    tmp_excel.write(response['Body'].read())
                    tmp_excel.flush()
                    
                    # Read the 'Sheet1' sheet from Excel file
                    try:
                        df = pd.read_excel(
                            tmp_excel.name, 
                            sheet_name='Sheet1',
                            engine='openpyxl'  # Explicitly specify the engine
                        )
                        print(f"Processed {obj['Key']}: {len(df)} rows")
                        
                        # Save to temporary parquet file
                        with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp_parquet:
                            df.to_parquet(tmp_parquet.name, index=False)
                            
                            # Upload to bronze bucket
                            parquet_key = f"{obj['Key'].rsplit('.', 1)[0]}.parquet"
                            with open(tmp_parquet.name, 'rb') as parquet_file:
                                s3.put_object(
                                    Bucket='bronze',
                                    Key=parquet_key,
                                    Body=parquet_file
                                )
                            print(f"Uploaded {parquet_key} to bronze bucket")
                    except Exception as e:
                        print(f"Error reading Excel file {obj['Key']}: {str(e)}")
            
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
    'excel_to_parquet',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['transform']
) as dag:

    convert_task = PythonOperator(
        task_id='convert_to_parquet',
        python_callable=convert_to_parquet
    )