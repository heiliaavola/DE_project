from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import json
import os
from pymongo import MongoClient
import logging

def process_excel_and_upload_to_mongodb(**context):
    """
    Read Excel file, convert to JSON files, and upload to MongoDB
    """
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        # Define paths
        input_file = '/opt/airflow/project_data/airflow/project_data/anonymized_data_package/metadata.xlsx'
        output_dir = '/opt/airflow/project_data/airflow/project_data/anonymized_data_package/metadata_json'
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Reading Excel file from: {input_file}")
        
        # Check if input file exists
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"Input file not found at: {input_file}")
        
        # Read Excel file
        df = pd.read_excel(input_file)
        logger.info(f"Successfully read Excel file with {len(df)} rows")
        
        # Connect to MongoDB
        mongo_conn_string = os.getenv('MONGODB_CONNECTION_STRING', 'mongodb://root:example@mongodb:27017/')
        client = MongoClient(mongo_conn_string)
        db = client['metadata_db']
        collection = db['machine_metadata']
        
        # Counter for processed documents
        processed_docs = 0
        
        # Convert rows to documents and insert into MongoDB
        for index, row in df.iterrows():
            # Get the JSON filename
            json_filename = f"{row['json_name']}.json"
            
            # Create the document data
            data = {
                "_id": row['json_name'],  # Use json_name as MongoDB document ID
                "operator_name": row['operator_name'],
                "operator_level": row['operator_level'],
                "machine_name": row['machine_name'],
                "machine_type": row['machine_type'],
                "electrode_material": row['electrode_material'],
                "measurement_name": row['measurement_name'],
                "filename": json_filename,  # Store the filename reference
                "created_at": datetime.utcnow()
            }
            
            # Save as JSON file
            json_path = os.path.join(output_dir, json_filename)
            json_data = {k: v for k, v in data.items() 
                        if k not in ['_id', 'filename', 'created_at']}  # Exclude MongoDB-specific fields
            
            with open(json_path, 'w') as json_file:
                json.dump(json_data, json_file, indent=4)
            logger.info(f"Generated JSON file: {json_filename}")
            
            try:
                # Insert document into MongoDB with custom _id
                result = collection.insert_one(data)
                processed_docs += 1
                logger.info(f"Inserted document with ID: {result.inserted_id}")
            except Exception as e:
                logger.error(f"Error inserting document {json_filename}: {str(e)}")
                # If document exists, update it
                if 'duplicate key error' in str(e):
                    collection.replace_one({'_id': row['json_name']}, data)
                    logger.info(f"Updated existing document: {json_filename}")
                else:
                    raise
        
        logger.info(f"Successfully processed and uploaded {processed_docs} documents")
        
        # Push the number of processed documents to XCom
        context['task_instance'].xcom_push(key='processed_docs_count', value=processed_docs)
        
        # Close MongoDB connection
        client.close()
        
        return f"Successfully processed and uploaded {processed_docs} documents"

    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        raise
"""
# Create the DAG
with DAG(
    'excel_to_mongodb',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['data_processing', 'mongodb'],
) as dag:

    process_and_upload = PythonOperator(
        task_id='process_excel_and_upload_to_mongodb',
        python_callable=process_excel_and_upload_to_mongodb,
        provide_context=True,
    )
    """

def get_convert_excel_metadata_to_json_to_mongodb_task(dag):
    return PythonOperator(
        task_id='process_excel_and_upload_to_mongodb',
        python_callable=process_excel_and_upload_to_mongodb,
        dag=dag
    )