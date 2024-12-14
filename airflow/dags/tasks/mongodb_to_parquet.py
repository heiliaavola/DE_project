from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pymongo import MongoClient
import boto3
import tempfile
import logging
import os

def convert_mongodb_to_parquet(**context):
    """
    Convert MongoDB documents to Parquet and save to MinIO bronze bucket,
    separated by machine
    """
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
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

        # Connect to MongoDB
        mongo_conn_string = os.getenv('MONGODB_CONNECTION_STRING', 'mongodb://root:example@mongodb:27017/')
        client = MongoClient(mongo_conn_string)
        db = client['metadata_db']
        collection = db['machine_metadata']

        # Get all documents from MongoDB
        documents = list(collection.find())
        logger.info(f"Retrieved {len(documents)} documents from MongoDB")

        # Group documents by machine and measurement_name
        machine_groups = {}
        for doc in documents:
            machine_name = doc['machine_name']
            measurement_name = doc['measurement_name']
            
            # Initialize machine group if it doesn't exist
            if machine_name not in machine_groups:
                machine_groups[machine_name] = {}
            
            # Initialize measurement group if it doesn't exist
            if measurement_name not in machine_groups[machine_name]:
                machine_groups[machine_name][measurement_name] = []
            
            # Remove MongoDB-specific fields
            clean_doc = {
                "operator_name": doc['operator_name'],
                "operator_level": doc['operator_level'],
                "machine_name": doc['machine_name'],
                "machine_type": doc['machine_type'],
                "electrode_material": doc['electrode_material'],
                "measurement_name": doc['measurement_name']
            }
            machine_groups[machine_name][measurement_name].append(clean_doc)

        # Convert and upload for each machine and measurement
        processed_files = 0
        for machine_name, measurements in machine_groups.items():
            machine_folder = f"{machine_name}_metadata"
            
            for measurement_name, docs in measurements.items():
                # Convert to DataFrame
                df = pd.DataFrame(docs)
                
                # Create temporary parquet file
                with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp:
                    df.to_parquet(tmp.name, index=False)
                    
                    # Define the key for the parquet file in MinIO
                    parquet_key = f"{machine_folder}/{measurement_name}.parquet"
                    
                    # Upload to bronze bucket
                    with open(tmp.name, 'rb') as parquet_file:
                        s3.put_object(
                            Bucket='bronze',
                            Key=parquet_key,
                            Body=parquet_file
                        )
                    
                    processed_files += 1
                    logger.info(f"Uploaded {parquet_key} to bronze bucket")

        # Verify bronze bucket contents for each machine
        try:
            logger.info("\nContents of bronze bucket:")
            for machine_name in machine_groups.keys():
                machine_folder = f"{machine_name}_metadata"
                response = s3.list_objects_v2(Bucket='bronze', Prefix=f"{machine_folder}/")
                logger.info(f"\n{machine_folder} contents:")
                for obj in response.get('Contents', []):
                    logger.info(f"- {obj['Key']} (Size: {obj['Size']} bytes)")
        except Exception as e:
            logger.error(f"Error listing bronze bucket contents: {str(e)}")

        logger.info(f"Successfully created and uploaded {processed_files} parquet files")
        
        # Close MongoDB connection
        client.close()
        
        return f"Successfully created and uploaded {processed_files} parquet files"

    except Exception as e:
        logger.error(f"Error converting to parquet: {str(e)}")
        raise

def get_mongodb_to_parquet_task(dag):
    return PythonOperator(
        task_id='mongodb_to_parquet',
        python_callable=convert_mongodb_to_parquet,
        dag=dag
    )

