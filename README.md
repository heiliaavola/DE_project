# VOLTAGE TO VALUE: ENGINEERING AN ANALYTICS PIPELINE FOR ELECTRODE DATA (DE_project)

Authors: Annabel Hiiu, Anett Sandberg, Heili Aavola

## Background

This data engineering project is based on measurement data originally provided by the R&D team at **OÜ Stargate Hydrogen Solutions**.  
The company manufactures electrolyzers, and the data focuses on measurements that compare different electrode materials.

The raw data, which has been pseudonymized for this project, primarily comes from two machines:
- **Machine 1**: Long-term and short-term measurements `.ndax` format (10 files)
- **Machine 2**: Measurements in `.dat` format (10 files)  
and metadata for each measurement in `.xlsx` format

### Key Questions to Answer

1. Which of the two electrode materials performs better at a certain current density?
2. How does data from different machines compare for the same electrode material?
3. Which electrode material showed faster degradation?
4. How does the purity/volume of produced H₂ compare between the two different electrode materials?

Data processing pipeline using Apache Airflow to transform machine data and metadata through various stages from raw data to star schema.

## Tools and Techonologies

- **Apache Airflow (2.7.1):** Workflow orchestration
- **MongoDB (7.0):** Document database for metadata storage
- **MinIO**: S3-compatible object storage
- **Apache Iceberg (1.6.0):** Table format for large analytic datasets
- **DuckDB**: Embedded analytics database
- **Python (3.12.7):** Programming language
- **Docker & Docker Compose:** Containerization
- **Jupyter Notebook:** Converting `.ndax` to `.xlsx` outside Docker
- **Data privacy:** More on that below 

## Prerequisites (this has been already done for you)

**Reading in ndax files and converting them to excel**  
- With Jupyter notebook `ndax_to_excel.ipynb` ndax files are read in from the `anonymized_data_package/machine_1` folder. 
- They are saved into `airflow/project_data/anonymized_data_package/machine_1`. 
- These excel files are read in by the first task of the pipeline `upload_raw_files`.

**Reason**  
- We have decided not to include this step in the Airflow orchestrated pipeline due to issue in Docker on MacOS. 
- The code uses package LimeNDAX that has a function get_records(). For this to work it needs to create temdata folder but creating and accessing this folder is a issue for MacOS.
- So excel files are already included in `airflow/project_data/anonymized_data_package/machine_1`.  

**Running the notebook**
- If you are brave (and have a Windows) then you can test it out (don't forget to `pip install LimeNDAX`) but to continue with pipeline you have to delete the duplicated excel files in the `airflow/project_data/anonymized_data_package/machine_1`.  

## Quick Start

1. Clone the repository

2. Start the containers:
```bash
docker compose up -d
```

3. Access Airflow UI:
- URL: http://localhost:8080
- Username: group8
- Password: group8

## Services Access

- **Airflow**: http://localhost:8080
  - Username: group8
  - Password: group8

- **MinIO**: http://localhost:9001
  - Username: minioadmin
  - Password: minioadmin

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

Note: Tasks 3-5 run sequentially to avoid resource deadlock errors (`[Errno 35]`)

6. **Parquet → Iceberg**
   - Convert Parquet files to Iceberg format
   - Stored in MinIO warehouse/silver bucket

7. **Iceberg → DuckDB**
   - Load Iceberg tables into DuckDB

8. **DuckDB → Star Schema**
   - Transform data into star schema for each machine
   - Separate transformations for machine_1 and machine_2 
   - We estimate it to take over an hour so we decided not to read in all the files yet 
   - Instead we read in only one file from machine_1 and machine_2 
   - We tried to overwrite temporary metadata with the right one from metadata files but it didn't work and we couldn't figure why

9. **Star Schema → Iceberg**
   - Store final star schema in Iceberg format
   - Stored in MinIO warehouse/gold bucket

## Data Flow Structure

```
NDAX → Excel → MongoDB/MinIO → Parquet → Iceberg → DuckDB → Star Schema → Iceberg
(Manual)        (Bronze)     (Bronze)  (Silver)          (Gold)       (Gold)
```

## Known Issues

1. **MacOS Issues**
   - Error: `[Errno 2] No such file or directory: '.\temdata'`
   - Error: `[Errno 35] Resource deadlock avoided`

2. **Running the pipeline again**

   1. Run `docker compose down`
   2. Delete all Docker images and volumes
   3. Run `docker compose up -d`

## Troubleshooting

- Check Airflow task logs in UI for failures
- Verify MinIO bucket and MongoDB database existence
- Ensure sufficient system resources
- For connectivity issues, verify all services are running:
```bash
docker ps
```

## Data Privacy

- **Pseudonymization Applied**:  
  The code `raw_data_anonymization.ipynb` replaces identifying data (like filenames and operator names) with artificial identifiers (e.g., `1001.ndax`, `2001.dat`, or `Operator_uuid`). A separate key file allows for the original data to be restored if necessary. The relationships between records are preserved and the actual measurement values remain unchanged.

- **Why It's Not Pure Anonymization**:  
  True anonymization would irreversibly sever the link between the data and its original identities. Here, the presence of a key file allows authorized re-identification, which means the process is reversible.

- **Why It's Not Data Masking**:  
  Data masking usually aims to hide sensitive data by transforming it into non-identifiable but still realistic values, often maintaining a similar data format. In contrast, this project uses sequential IDs and UUIDs, focusing on privacy protection over preserving realistic values in the identifiers.

**Benefits of Pseudonymization**:

- Protects privacy while maintaining data utility.
- Preserves the structure and relationships within the dataset.
- Complies with certain privacy regulations by allowing for authorized re-identification when necessary.
- Facilitates research and analysis without exposing the original sensitive information → a key factor in gaining approval from **OÜ Stargate Hydrogen Solutions** to use the data.

