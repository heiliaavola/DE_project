from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import duckdb
from pyiceberg.catalog import load_catalog

def iceberg_to_duckdb(**context):
    """Read Iceberg tables using catalog and load them into DuckDB."""
    # Initialize DuckDB connection
    duckdb_path = "/app/data/warehouse.duckdb" # kahtlane, ei saa otseselt aru, et miks just see
    conn = duckdb.connect(duckdb_path)
    
    try:
        # Configure DuckDB S3 settings
        conn.execute("""
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_region='us-east-1';
            SET s3_url_style='path';
            SET s3_endpoint='minio:9000';
            SET s3_access_key_id='minioadmin';
            SET s3_secret_access_key='minioadmin';
            SET s3_use_ssl=false;
        """)
        
        # Install and load Iceberg extension
        conn.execute("INSTALL iceberg")
        conn.execute("LOAD iceberg")
        
        # Load Iceberg catalog
        try:
            catalog = load_catalog(name="rest") # catalogi loomine on failis mnt/tmp/duckdb_data/.pyiceberg.yaml
        except Exception as e:
            print(f"Problems with catalog: {e}")
        
        try:
            # List all tables in the default namespace
            tables = catalog.list_tables("default") # default on namespace nimi - võtsin praksist
            
            for namespace, table_name in tables:
                try:
                    # Construct full table identifier
                    full_table_name = f"{namespace}.{table_name}"
                    
                    print(f"Processing Iceberg table: {full_table_name}")
                    
                    # Load table from catalog
                    iceberg_table = catalog.load_table((namespace, table_name))
                    
                    # Get table location
                    table_location = iceberg_table.location()
                    print(f"Table location: {table_location}")
                    
                    # Create DuckDB table from Iceberg data
                    create_table_sql = f"""
                        CREATE OR REPLACE TABLE {table_name} AS 
                        SELECT * FROM iceberg_scan(
                            '{table_location}',
                            aws_access_key_id='minioadmin',
                            aws_secret_access_key='minioadmin',
                            endpoint='minio:9000',
                            region='us-east-1',
                            url_style='path',
                            use_ssl=false
                        );
                    """
                    
                    conn.execute(create_table_sql)
                    print(f"Successfully loaded {table_name} into DuckDB")
                    
                    # Verify the data
                    row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    print(f"Table {table_name} contains {row_count} rows")
                    
                    # Show sample of data
                    print(f"Sample of data from {table_name}:")
                    conn.execute(f"SELECT * FROM {table_name} LIMIT 5").show()
                    
                except Exception as e:
                    print(f"Error processing table {full_table_name}: {str(e)}")
                    raise
                    
        except Exception as e:
            print(f"Error listing tables from catalog: {str(e)}")
            raise
            
    finally:
        conn.close()

# Create the DAG
with DAG(
    'iceberg_to_duckdb',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['transform']
) as dag:

    load_to_duckdb_task = PythonOperator(
        task_id='iceberg_to_duckdb',
        python_callable=iceberg_to_duckdb
    )