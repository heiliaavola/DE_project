from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import duckdb

def write_to_duckdb():
    con = duckdb.connect('/app/data/example.duckdb')
    con.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER, name VARCHAR)")
    con.execute("INSERT INTO test VALUES (4, 'test4')")

with DAG('write_to_duckdb',
         default_args={'start_date': datetime(2023, 1, 1)},
         schedule_interval=None,
         catchup=False) as dag:

    task = PythonOperator(
        task_id='write_task',
        python_callable=write_to_duckdb
    )
