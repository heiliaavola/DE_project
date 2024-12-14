from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import duckdb
from pyiceberg.catalog import load_catalog
import pyarrow as pa

def create_star_schema(**context):
    """Create star schema tables in DuckDB."""
    duckdb_path = "/app/data/warehouse.duckdb"
    conn = duckdb.connect(duckdb_path)

    try:
        # Create dimension tables
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_machine (
            machine_id INTEGER NOT NULL PRIMARY KEY,
            machine_name VARCHAR(100) NOT NULL UNIQUE,
            machine_type VARCHAR(50) NOT NULL
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_measurement (
                measurement_id INTEGER NOT NULL PRIMARY KEY,
                measurement_name VARCHAR(100) NOT NULL UNIQUE,
                start_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                finish_timestamp TIMESTAMP WITH TIME ZONE NOT NULL
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_date (
                date_id INTEGER NOT NULL,
                date DATE NOT NULL UNIQUE,
                day BIGINT NOT NULL,
                month BIGINT NOT NULL,
                year BIGINT NOT NULL,
                PRIMARY KEY (date_id)
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_time (
                time_id INTEGER NOT NULL,
                time TIME NOT NULL UNIQUE,
                hour BIGINT NOT NULL,
                minute BIGINT NOT NULL,
                second BIGINT NOT NULL,
                PRIMARY KEY (time_id)
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_operator (
                operator_id INTEGER NOT NULL,
                operator_name VARCHAR(100) NOT NULL UNIQUE,
                operator_level VARCHAR(50) NOT NULL,
                PRIMARY KEY (operator_id)
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_electrode_material (
                electrode_material_id INTEGER NOT NULL,
                electrode_material VARCHAR(100) NOT NULL UNIQUE,
                PRIMARY KEY (electrode_material_id)
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_dat_metadata (
                dat_metadata_id INTEGER,
                diaphragm VARCHAR(100),
                thickness BIGINT,
                porosity BIGINT,
                electrolyte VARCHAR(100),
                electrodes VARCHAR(100),
                electrodes_area NUMERIC(10,3),
                current_density_mode VARCHAR(50),
                current_density_value NUMERIC(10,2),
                pump_speed_mode VARCHAR(50),
                pump_speed_value NUMERIC(10,2),
                experiment_running_time NUMERIC(10,2),
                PRIMARY KEY (dat_metadata_id)
            );
        """)

        # Create fact tables
        conn.execute("""DROP TABLE IF EXISTS fact_excel_measurements""")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fact_excel_measurements (
                fact_excel_measurement_id INTEGER NOT NULL,
		        machine_id BIGINT NOT NULL,
                measurement_id BIGINT NOT NULL,
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
		        date_id BIGINT NOT NULL,
                time_id BIGINT NOT NULL,
		        electrode_material_id BIGINT NOT NULL,
		        voltage_v NUMERIC(10,4),                --using same data types in both fact tables, so later joins will be easier
                current_a NUMERIC(10,4),                --using same data types in both fact tables, so later joins will be easier
                time_in_step NUMERIC(18,8) NOT NULL,
                record_id BIGINT NOT NULL,
                cycle BIGINT NOT NULL,
                step_id BIGINT NOT NULL,
                step_name VARCHAR(50) NOT NULL,
                capacity_mah NUMERIC(18,8),
                energy_mwh NUMERIC(18,8),
		        operator_id BIGINT NOT NULL,		
                PRIMARY KEY (fact_excel_measurement_id)
            );
        """)


        print("Star schema created successfully.")

    finally:
        conn.close()

def populate_star_schema(**kwargs):
    """Populate star schema tables with data in DuckDB."""
    duckdb_path = "/app/data/warehouse.duckdb"
    conn = duckdb.connect(duckdb_path)
    print("Beginning of populating the star schema tables with data.")

    try:
        tables = conn.execute("SHOW TABLES;").fetchall()

        # 1. INSERTING DATA INTO dim_machine
        for table in tables :
            table_name = table[0]
            if table_name[0].isdigit() and table_name.startswith("1"): 
                # Extract the first number as machine_id
                machine_id = int(table_name[0])
                machine_name = f"machine_{machine_id}"
                machine_type = f"machine_{machine_id}"
                
                # Check if the machine_name already exists in dim_machine
                existing_machine = conn.execute(
                    "SELECT 1 FROM dim_machine WHERE machine_name = ?", (machine_name,)
                ).fetchone()
                
                if not existing_machine:  # Only insert if the machine_name does not exist
                    # Determine the next available machine_id
                    max_machine_id = conn.execute(
                        "SELECT COALESCE(MAX(machine_id), 0) FROM dim_machine;"
                    ).fetchone()[0]
                    next_machine_id = max_machine_id + 1
                    
                    # Insert into dim_machine using parameterized query
                    conn.execute("""
                        INSERT INTO dim_machine (machine_id, machine_name, machine_type)
                        VALUES (?, ?, ?);
                    """, (next_machine_id, machine_name, machine_type))
                    print(f"Inserted machine_id={next_machine_id} for machine {machine_name} into dim_machine.")

        # Fetch and display the contents of dim_machine
        results = conn.execute("SELECT * FROM dim_machine LIMIT 2;").fetchall()
        print("Dimension dim_machine created. Example rows:")
        for row in results:
            print(row)
 

        # 2. INSERTING DATA INTO dim_machine
        for table in tables:
            table_name = table[0]
            if table_name[0].isdigit() and table_name.startswith("1"): 

                # Extract measurement_name from the table name
                measurement_name = table_name.split("_")[0]

                existing_measurement = conn.execute(
                    "SELECT 1 FROM dim_measurement WHERE measurement_name = ?", (measurement_name,)
                ).fetchone()

                #Skip if the measurement_name already exists
                if existing_measurement:  
                    continue

                # Determine the next available measurement_id
                max_measurement_id = conn.execute(
                    "SELECT COALESCE(MAX(measurement_id), 0) FROM dim_measurement;"
                ).fetchone()[0]
                next_measurement_id = max_measurement_id + 1

                # Fetch the earliest and latest timestamps from the table
                query = f"""
                    SELECT 
                        MIN(adjusted_timestamp) AS start_timestamp,
                        MAX(adjusted_timestamp) AS finish_timestamp
                    FROM "{table_name}"
                """
                timestamps = conn.execute(query).fetchone()
                start_timestamp, finish_timestamp = timestamps

                # Insert into dim_measurement using parameterized query
                conn.execute("""
                    INSERT INTO dim_measurement (measurement_id, measurement_name, start_timestamp, finish_timestamp)
                    VALUES (?, ?, ?, ?);
                """, (next_measurement_id, measurement_name, start_timestamp, finish_timestamp))
                print(f"Inserted measurement_id={next_measurement_id} for measurement {measurement_name} into dim_measurement.")

        # Fetch and display the contents of dim_measurement
        results = conn.execute("SELECT * FROM dim_measurement LIMIT 2;").fetchall()
        print("Dimension dim_measurement created. Example rows:")
        for row in results:
            print(row)

        # 3. INSERTING DATA INTO dim_date
        for table in tables:
            table_name = table[0]
            # Filter for machine_1 tables by their naming convention (starting with "1")
            if table_name[0].isdigit() and table_name.startswith("1"): 
                # Extract distinct dates from the adjusted_timestamp column
                query = f"""
                    SELECT DISTINCT CAST(adjusted_timestamp AS DATE) AS date
                    FROM "{table_name}"
                """
                dates = conn.execute(query).fetchall()

                for date_row in dates:
                    date = date_row[0]  # Extract the date value

                    # Check if the date already exists in dim_date
                    existing_date = conn.execute(
                        "SELECT 1 FROM dim_date WHERE date = ?", (date,)
                    ).fetchone()

                    if existing_date:  # Skip if the date already exists
                        continue

                    # Determine the next available date_id
                    max_date_id = conn.execute(
                        "SELECT COALESCE(MAX(date_id), 0) FROM dim_date;"
                    ).fetchone()[0]
                    next_date_id = max_date_id + 1

                    # Extract year, month, and day from the date
                    day = date.day
                    month = date.month
                    year = date.year

                    # Insert into dim_date using a parameterized query
                    conn.execute("""
                        INSERT INTO dim_date (date_id, date, day, month, year)
                        VALUES (?, ?, ?, ?, ?);
                    """, (next_date_id, date, day, month, year))

        # Fetch and display the contents of dim_date
        results = conn.execute("SELECT * FROM dim_date LIMIT 2;").fetchall()
        print("Dimension dim_date created. Example rows:")
        for row in results:
            print(row)

        # 4. INSERTING DATA INTO dim_time
        for table in tables:
            table_name = table[0]
            # Filter for machine_1 tables by their naming convention (starting with "1")
            if table_name[0].isdigit() and table_name.startswith("1"):  
                # Extract distinct times (hour, minute, second) from the adjusted_timestamp column
                query = f"""
                    SELECT DISTINCT 
                        CAST(adjusted_timestamp AS TIME) AS time,
                        EXTRACT(HOUR FROM adjusted_timestamp) AS hour,
                        EXTRACT(MINUTE FROM adjusted_timestamp) AS minute,
                        EXTRACT(SECOND FROM adjusted_timestamp) AS second
                    FROM "{table_name}"
                """
                times = conn.execute(query).fetchall()

                for time_row in times:
                    time, hour, minute, second = time_row  # Extract the values
        
                    # Check if the time already exists in dim_time
                    existing_time = conn.execute(
                        "SELECT 1 FROM dim_time WHERE time = ?", (time,)
                    ).fetchone()

                    if existing_time:  # Skip if the time already exists
                        continue

                    # Determine the next available time_id
                    max_time_id = conn.execute(
                        "SELECT COALESCE(MAX(time_id), 0) FROM dim_time;"
                    ).fetchone()[0]
                    next_time_id = max_time_id + 1

                    # Insert into dim_time using parameterized query
                    conn.execute("""
                        INSERT INTO dim_time (time_id, time, hour, minute, second)
                        VALUES (?, ?, ?, ?, ?);
                    """, (next_time_id, time, hour, minute, second))

        # Fetch and display the contents of dim_time
        results = conn.execute("SELECT * FROM dim_time LIMIT 2;").fetchall()
        print("Dimension dim_time created. Example rows:")
        for row in results:
            print(row)

        # 5. INSERTING DATA INTO dim_operator
        for table in tables:
            table_name = table[0]
            # Filter for machine_1 tables by their naming convention (starting with "1")
            if table_name[0].isdigit() and table_name.startswith("1"):  
                # Always use fixed values for operator_name and operator_level
                operator_name = 'Operator_2d226185_e3f33b7d'
                operator_level = 'medium'

                # Check if the operator already exists in dim_operator
                existing_operator = conn.execute(
                    "SELECT 1 FROM dim_operator WHERE operator_name = ?;",
                    (operator_name,)
                ).fetchone()

                if not existing_operator:  # Insert only if the operator does not exist
                    # Determine the next available operator_id
                    max_operator_id = conn.execute(
                        "SELECT COALESCE(MAX(operator_id), 0) FROM dim_operator;"
                    ).fetchone()[0]
                    next_operator_id = max_operator_id + 1

                    # Insert into dim_operator
                    conn.execute("""
                        INSERT INTO dim_operator (operator_id, operator_name, operator_level)
                        VALUES (?, ?, ?);
                    """, (next_operator_id, operator_name, operator_level))
                    print(f"Inserted operator_id={next_operator_id} with operator_name '{operator_name}' into dim_operator.")

        # Fetch and display the contents of dim_operator
        results = conn.execute("SELECT * FROM dim_operator LIMIT 5;").fetchall()
        print("Dimension dim_operator created. Example rows:")
        for row in results:
            print(row)


        # 6. INSERTING DATA INTO dim_electrode_material
        for table in tables:
            table_name = table[0]
            # Filter for machine_1 tables by their naming convention (starting with "1")
            if table_name[0].isdigit() and table_name.startswith("1"):
                # Always use the fixed value for electrode_material
                electrode_material = '1'

                # Check if the electrode material already exists in dim_electrode_material
                existing_material = conn.execute(
                    "SELECT 1 FROM dim_electrode_material WHERE electrode_material = ?;",
                    (electrode_material,)
                ).fetchone()

                if not existing_material:  # Insert only if the electrode material does not exist
                    # Determine the next available electrode_material_id
                    max_electrode_material_id = conn.execute(
                        "SELECT COALESCE(MAX(electrode_material_id), 0) FROM dim_electrode_material;"
                    ).fetchone()[0]
                    next_electrode_material_id = max_electrode_material_id + 1

                    # Insert into dim_electrode_material
                    conn.execute("""
                        INSERT INTO dim_electrode_material (electrode_material_id, electrode_material)
                        VALUES (?, ?);
                    """, (next_electrode_material_id, electrode_material))
                    print(f"Inserted electrode_material_id={next_electrode_material_id} with electrode_material '{electrode_material}' into dim_electrode_material.")

        # Fetch and display the contents of dim_electrode_material
        results = conn.execute("SELECT * FROM dim_electrode_material LIMIT 5;").fetchall()
        print("Dimension dim_electrode_material created. Example rows:")
        for row in results:
            print(row)

        #7. Insert data into fact_excel_measurements in batches
        for table in tables:
            table_name = table[0]
            # Filter for machine_1 tables by their naming convention (starting with "1")
            if table_name[0].isdigit() and table_name.startswith("1001"): #PRAEGU AINULT 1 FAIL
                print(f"Processing fact data insertion for table {table_name}")

                # Query to fetch all rows from the table
                query = f"""
                    SELECT 
                        adjusted_timestamp AS timestamp,
                        record_ID AS record_id,
                        cycle,
                        step_ID AS step_id,
                        step_name,
                        time_in_step,
                        CASE 
                            WHEN voltage_V = 'NAN' OR voltage_V IS NULL THEN NULL
                            ELSE ROUND(CAST(voltage_V AS NUMERIC(10,4)), 4) -- Round voltage_v to NUMERIC(10,4)
                        END AS voltage_v,
                        CASE 
                            WHEN current_mA = 'NAN' OR current_mA IS NULL THEN NULL
                            ELSE ROUND(CAST(current_mA / 1000 AS NUMERIC(10,4)), 4) -- Convert to amps and round to NUMERIC(10,4)
                        END AS current_a,
                        CASE 
                            WHEN capacity_mAh = 'NAN' OR capacity_mAh IS NULL THEN NULL
                            ELSE CAST(capacity_mAh AS NUMERIC(18,8))
                        END AS capacity_mah,
                        CASE 
                            WHEN energy_mWh = 'NAN' OR energy_mWh IS NULL THEN NULL
                            ELSE CAST(energy_mWh AS NUMERIC(18,8))
                        END AS energy_mwh
                    FROM "{table_name}"
                """

                rows = conn.execute(query).fetchall()

                # Fetch starting fact_excel_measurement_id
                max_fact_id = conn.execute(
                    "SELECT COALESCE(MAX(fact_excel_measurement_id), 0) FROM fact_excel_measurements;"
                ).fetchone()[0]

                # Batch insert setup
                batch_size = 500
                batch = []

                for row in rows:
                    # Increment fact_excel_measurement_id
                    max_fact_id += 1

                    # Extract row data
                    (timestamp, record_id, cycle, step_id, step_name, time_in_step, voltage_v, current_a, capacity_mah, energy_mwh) = row

                    # Resolve IDs for dimensions
                    machine_id = conn.execute("SELECT machine_id FROM dim_machine WHERE machine_name = ?", 
                                               (f"machine_{table_name[0]}",)).fetchone()[0]
                    measurement_id = conn.execute("SELECT measurement_id FROM dim_measurement WHERE measurement_name = ?", 
                                                  (table_name.split("_")[0],)).fetchone()[0]
                    operator_id = conn.execute("SELECT operator_id FROM dim_operator WHERE operator_name = ?", 
                                                ('Operator_2d226185_e3f33b7d',)).fetchone()[0]
                    date_id = conn.execute("SELECT date_id FROM dim_date WHERE date = ?", (timestamp.date(),)).fetchone()[0]
                    time_id = conn.execute("SELECT time_id FROM dim_time WHERE time = ?", (timestamp.time(),)).fetchone()[0]
                    electrode_material_id = conn.execute(
                        "SELECT electrode_material_id FROM dim_electrode_material WHERE electrode_material = ?;",
                        ('1',)
                    ).fetchone()[0]

                    # Append the row to the batch
                    batch.append((
                        max_fact_id, machine_id, measurement_id, timestamp, date_id, time_id, 
                        electrode_material_id, voltage_v, current_a, time_in_step, record_id, 
                        cycle, step_id, step_name, capacity_mah, energy_mwh, operator_id
                    ))


                    # Insert batch when batch size is met
                    if len(batch) >= batch_size:
                        conn.executemany("""
                            INSERT INTO fact_excel_measurements (
                                fact_excel_measurement_id, machine_id, measurement_id, timestamp, date_id, time_id,
                                electrode_material_id, voltage_v, current_a, time_in_step, record_id, cycle, 
                                step_id, step_name, capacity_mah, energy_mwh, operator_id
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """, batch)
                        batch = []

                # Insert remaining rows in the batch
                if batch:
                    conn.executemany("""
                        INSERT INTO fact_excel_measurements (
                            fact_excel_measurement_id, machine_id, measurement_id, timestamp, date_id, time_id,
                            electrode_material_id, voltage_v, current_a, time_in_step, record_id, cycle, 
                            step_id, step_name, capacity_mah, energy_mwh, operator_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """, batch)

                print(f"Finished inserting rows from table {table_name} into fact_excel_measurements.")

        # Fetch and display the contents of fact_excel_measurements
        results = conn.execute("SELECT * FROM fact_excel_measurements LIMIT 5;").fetchall()
        print("Fact table fact_excel_measurements created. Example rows:")
        for row in results:
            print(row)
        print("Fact table fact_excel_measurements created. Example rows:")
        for row in results:
            print(f"""
                fact_excel_measurement_id: {row[0]}, machine_id: {row[1]}, measurement_id: {row[2]}, 
                timestamp: {row[3]}, date_id: {row[4]}, time_id: {row[5]}, 
                electrode_material_id: {row[6]}, voltage_v: {row[7]}, current_a: {row[8]}, 
                time_in_step: {row[9]}, record_id: {row[10]}, cycle: {row[11]}, 
                step_id: {row[12]}, step_name: {row[13]}, capacity_mah: {row[14]}, 
                energy_mwh: {row[15]}, operator_id: {row[16]}
            """)


        

  
    finally:
        conn.close()



def duckdb_to_star_schema_machine_1(**kwargs):
    """Create star schema and populate it with data from DuckDB."""
    create_star_schema(**kwargs)
    populate_star_schema(**kwargs)

"""
# Create the DAG
with DAG(
    'duckdb_to_star_schema_machine_1',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['transform']
) as dag:

    duckdb_to_star_schema_machine_1 = PythonOperator(
        task_id='duckdb_to_star_schema_machine_1',
        python_callable=duckdb_to_star_schema_machine_1
    )
"""

def get_duckdb_to_star_schema_machine_1_task(dag):
    return PythonOperator(
        task_id='duckdb_to_star_schema_machine_1',
        python_callable=duckdb_to_star_schema_machine_1,
        dag=dag
    )