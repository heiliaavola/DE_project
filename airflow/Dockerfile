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

RUN pip install --upgrade pip && \
    pip install --no-cache-dir \
    duckdb \
    "pyarrow>=10.0.1,<10.1.0" \
    boto3 \
    pyiceberg \
    thrift \
    openpyxl