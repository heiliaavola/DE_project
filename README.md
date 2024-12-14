# DE_Project

Data processing pipeline using Apache Airflow to transform machine data and metadata into Apache Iceberg format.

## Prerequisites

- Docker and Docker Compose
- At least 4GB RAM

## Quick Start

1. Start the containers:
```bash
docker compose up -d
```

2. Access Airflow UI:
- URL: http://localhost:8080
- Username: group8
- Password: group8

## Project Structure
```
DE_PROJECT/
├── airflow/
│   ├── dags/
│   │   ├── tasks/           # Individual pipeline tasks
│   │   └── pipeline_dag.py  # Main DAG definition
│   └── Dockerfile
└── project_data/
    └── anonymized_data_package/
        ├── machine_1/
        ├── machine_1_metadata/
        ├── machine_2/
        └── metadata.xlsx
```

## Pipeline Flow

1. **Upload Raw Files**
   - Uploads .xlsx and .dat files from project_data/anonymized_data_package to MinIO (data/raw-data)

2. **Excel Metadata to MongoDB**
   - Converts Excel files to JSON
   - Stores as BSON in MongoDB

3. **Data Transformations**
   - Excel to Parquet: Converts machine data Excel files
   - DAT to Parquet: Converts machine2 .dat files to Parquet (data/bronze/)
   - MongoDB to Parquet: Converts stored BSON files to Parquet

4. **Parquet to Iceberg**
   - Converts all Parquet files (machine_1, machine_2, metadata) to Iceberg format

5. **Iceberg to DuckDB**
   - Work in progress

## Services

- **Airflow**: http://localhost:8080
- **MinIO**: http://localhost:9001 (minioadmin/minioadmin)
- **MongoDB**: localhost:27017 (root/example)

## Troubleshooting

- Check Airflow task logs in UI for failures
- Verify MinIO bucket and MongoDB database existence
- Ensure sufficient system resources