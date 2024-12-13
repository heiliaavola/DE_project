from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
import warnings
from LimeNDAX import get_records, ndax_basic
import tempfile

# Add the missing status code
ndax_basic.state_dict[25] = "Unknown_State_25"

def safe_convert_ndax(file_path, output_path):
    """
    Safely convert a single NDAX file to Excel with enhanced error handling.
    """
    try:
        # Create a temporary directory for processing
        with tempfile.TemporaryDirectory() as temp_dir:
            # Set environment variable for temporary files
            original_temp = os.environ.get('TEMP')
            os.environ['TEMP'] = temp_dir
            
            try:
                # Attempt to read the file
                df = get_records(file_path)
                
                if df is not None and not df.empty:
                    # Clean the data
                    df_clean = df.copy()
                    for column in df_clean.columns:
                        if df_clean[column].dtype == 'object':
                            df_clean[column] = df_clean[column].fillna('')
                    
                    # Save to Excel
                    df_clean.to_excel(output_path, index=False)
                    return True
                
            finally:
                # Restore original temp directory
                if original_temp:
                    os.environ['TEMP'] = original_temp
                    
    except Exception as e:
        print(f"Error converting {os.path.basename(file_path)}: {str(e)}")
        return False

def convert_ndax_files(**context):
    """
    Convert all NDAX files in the input folder to Excel files.
    """
    # Define input and output paths
    input_folder = '/opt/airflow/project_data/anonymized_data_package/machine_1'
    output_folder = '/opt/airflow/project_data/anonymized_data_package/machine_1/excel_output'
    
    # Create output directory if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)
    
    # Track conversions
    successful = []
    failed = []
    
    # Suppress warnings
    warnings.filterwarnings('ignore', category=FutureWarning)
    warnings.filterwarnings('ignore', category=UserWarning)
    
    # Process each file
    for filename in os.listdir(input_folder):
        if filename.lower().endswith('.ndax'):
            input_path = os.path.join(input_folder, filename)
            output_path = os.path.join(output_folder, os.path.splitext(filename)[0] + '.xlsx')
            
            print(f"\nProcessing: {filename}")
            if safe_convert_ndax(input_path, output_path):
                successful.append(filename)
                print(f"Successfully converted: {filename}")
            else:
                failed.append(filename)
                print(f"Failed to convert: {filename}")
    
    # Print summary
    summary = {
        'total_files': len(successful) + len(failed),
        'successful': len(successful),
        'failed': len(failed),
        'failed_files': failed
    }
    
    return summary

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Create the DAG
dag = DAG(
    'ndax_to_excel_conversion',
    default_args=default_args,
    description='Convert NDAX files to Excel format',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 12, 13),
    catchup=False,
    tags=['ndax', 'conversion']
)

# Define the conversion task
convert_task = PythonOperator(
    task_id='convert_ndax_files',
    python_callable=convert_ndax_files,
    provide_context=True,
    dag=dag
)