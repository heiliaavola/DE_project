# VOLTAGE TO VALUE: ENGINEERING AN ANALYTICS PIPELINE FOR ELECTRODE DATA (DE_project)

Data processing pipeline using Apache Airflow to transform machine data and metadata through various stages from raw data to star schema.

## Prerequisites

1. **Important**: Run `ndax_to_excel.ipynb` Jupyter notebook first
   - This converts NDAX files to Excel format
   - This step is separate from Airflow due to issue in Docker on MacOS. Tempdata directory access errors when processing NDAX files

Using Jupyter notebook locally bypasses these Docker/MacOS file system limitations

## Quick Start

1. Start the containers:
```bash
docker compose up -d
```

2. Access Airflow UI:
- URL: http://localhost:8080
- Username: group8
- Password: group8

## Services Access

- **Airflow**: http://localhost:8080
  - Username: group8
  - Password: group8

- **MinIO**: http://localhost:9001
  - Username: group8
  - Password: group8

- **MongoDB**: localhost:27017
  - Username: group8
  - Password: group8

## Pipeline Flow

1. **Upload Raw Files**
   - Source: project_data/anonymized_data_package
   - Destination: MinIO (data/raw-data)
   - Handles: .xlsx and .dat files

2. **Excel Metadata → MongoDB**
   - Converts Excel metadata to JSON
   - Stores as BSON in MongoDB

3. **Excel → Parquet**
   - Converts machine_1 Excel files to Parquet format
   - Stored in MinIO bronze bucket

4. **DAT → Parquet**
   - Converts machine_2 .dat files to Parquet
   - Stored in MinIO bronze bucket

5. **MongoDB → Parquet**
   - Converts metadata from MongoDB to Parquet
   - Stored in MinIO bronze bucket

6. **Parquet → Iceberg**
   - Convert Parquet files to Iceberg format

7. **Iceberg → DuckDB**
   - Load Iceberg tables into DuckDB

8. **DuckDB → Star Schema**
   - Transform data into star schema for each machine
   - Separate transformations for machine_1 and machine_2

9. **Star Schema → Iceberg**
   - Store final star schema in Iceberg format

## Data Flow Structure

```
NDAX → Excel → MongoDB/MinIO → Parquet → Iceberg → DuckDB → Star Schema → Iceberg
(Manual)        (Bronze)     (Bronze)  (Silver)          (Gold)       (Gold)
```

## Known Issues

1. **MacOS Issues**
   - Error: `[Errno 2] No such file or directory: '.\temdata'`
   - Error: `[Errno 35] Resource deadlock avoided`
   - Impact: NDAX to Excel conversion fails in Docker containers on MacOS

## Troubleshooting

- Check Airflow task logs in UI for failures
- Verify MinIO bucket and MongoDB database existence
- Ensure sufficient system resources