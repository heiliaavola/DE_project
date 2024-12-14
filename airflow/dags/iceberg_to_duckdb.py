from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import duckdb
from pyiceberg.catalog import load_catalog
import pyarrow as pa

def iceberg_to_duckdb(**context):
    """Read Iceberg tables using catalog and load them into DuckDB."""
    # Initialize DuckDB connection
    duckdb_path = "/app/data/warehouse.duckdb"
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
            catalog = load_catalog(name="rest")
        except Exception as e:
            print(f"Problems with catalog: {e}")

        try:
            # List all tables in the default namespace
            tables = catalog.list_tables("default")

            # Process each Iceberg table
            for namespace, table_name in catalog.list_tables("default"):
                try:
                    full_table_name = f"{namespace}.{table_name}"
                    print(f"Processing Iceberg table: {full_table_name}")

                    iceberg_table = catalog.load_table((namespace, table_name))
                    arrow_table = iceberg_table.scan().to_arrow()

                    # Register Arrow table with DuckDB and save it as a separate table
                    conn.register("temp_arrow_table", arrow_table)
                    # Quote the table name to handle numeric or special characters
                    duckdb_table_name = f'"{table_name.replace(".", "_")}"'
                    conn.execute(f"CREATE OR REPLACE TABLE {duckdb_table_name} AS SELECT * FROM temp_arrow_table")

                    print(f"Saved table {full_table_name} as {duckdb_table_name} in DuckDB")

            
                    # Show sample data for each table in the database
                    tables = conn.execute('SHOW TABLES').fetchall()
                    print("Tables in DuckDB:")
                    for table in tables:
                        # Extract the table name
                        table_name = table[0]

                        # Print first 20 rows
                        print(f"\nFirst 20 rows of table  {table_name}:")
                        try:
                           # rows = conn.execute(f'SELECT * FROM "{table_name}" LIMIT 20').fetchall()
                            #conn.execute(f'SELECT "Voltage [V]" FROM "{table_name}" LIMIT 20').fetchall()
                            #for row in rows:
                            #    print(row)
                         # Fetch a specific column (e.g., "Voltage [V]")
                            specific_column_data = conn.execute(f'SELECT "Voltage [V]" FROM "{table_name}" LIMIT 20').fetchall()
                            print(f"\nFirst 20 rows of column 'Voltage [V]' in table {table_name}:")
                            for row in specific_column_data:
                                print(row)
                        except Exception as e:
                            print(f"Error fetching rows from table {table_name}: {e}")
    

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
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['transform']
) as dag:

    iceberg_to_duckdb = PythonOperator(
        task_id='iceberg_to_duckdb',
        python_callable=iceberg_to_duckdb
    )