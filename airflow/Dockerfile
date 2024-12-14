# Airflow Dockerfile
FROM apache/airflow:2.7.1-python3.9

USER root
RUN apt-get update && apt-get install -y \
    curl \
    unzip && \
    curl -L https://github.com/duckdb/duckdb/releases/download/v1.1.1/duckdb_cli-linux-amd64.zip -o duckdb.zip && \
    unzip duckdb.zip && \
    mv duckdb /usr/local/bin/duckdb && \
    chmod +x /usr/local/bin/duckdb && \
    rm duckdb.zip && \
    apt-get clean

USER airflow

RUN pip install --upgrade pip
#Anetil töötab (-> parquet to iceberg ERROR: Iceberg does not yet support 'ns' timestamp precision)
RUN pip install pyarrow==18.1.0 pyiceberg==0.8.0 duckdb boto3 thrift openpyxl 

#Ima pyicebergi versioonita (-> parquet to iceberg ERROR: Iceberg does not yet support 'ns' timestamp precision)
#RUN pip install pyarrow==18.1.0 pyiceberg duckdb boto3 thrift openpyxl 

#Annabeli versioon (-> iceberg_to_duckdb ERROR: concat_tables() got an unexpected keyword argument 'promote_options')
#RUN pip install --no-cache-dir \
#    duckdb \
#    "pyarrow>=10.0.1,<10.1.0" \
#    boto3 \
#    pyiceberg \
#    thrift \
#    openpyxl