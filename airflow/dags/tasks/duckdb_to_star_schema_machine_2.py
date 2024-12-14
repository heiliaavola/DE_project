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

        conn.execute("""
            CREATE TABLE IF NOT EXISTS fact_dat_measurements (
                fact_dat_measurement_id INTEGER NOT NULL,
                machine_id BIGINT NOT NULL,
                measurement_id BIGINT NOT NULL,
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                date_id BIGINT NOT NULL,
                time_id BIGINT NOT NULL,
                electrode_material_id BIGINT NOT NULL,
                dat_metadata_id BIGINT,
                voltage_v NUMERIC(10,4),
		        current_a NUMERIC(10,4),
                h2_flow NUMERIC(10,4),
                o2_flow NUMERIC(10,4),
                h2_purity_percent NUMERIC(10,4),
                o2_purity_percent NUMERIC(10,4),
                h2_pressure_bar NUMERIC(10,4),
                o2_pressure_bar NUMERIC(10,4),
                h2_level_percent NUMERIC(10,4),
                o2_level_percent NUMERIC(10,4),
		        set_pressure_bar NUMERIC(10,4),
                cathode_temp_c NUMERIC(10,4),
                anode_temp_c NUMERIC(10,4),
                cell_inlet_temp_c NUMERIC(10,4),
		        operator_id BIGINT,
                PRIMARY KEY (fact_dat_measurement_id)
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
            if table_name[0].isdigit() and table_name.startswith("2"): 
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
            if table_name[0].isdigit() and table_name.startswith("2"): 

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
                        MIN(TimeStamp) AS start_timestamp,
                        MAX(TimeStamp) AS finish_timestamp
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
            if table_name[0].isdigit() and table_name.startswith("2"): 
                # Extract distinct dates from the table
                query = f"""
                    SELECT DISTINCT CAST(TimeStamp AS DATE) AS date
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

                    # Insert into dim_date using parameterized query
                    conn.execute("""
                        INSERT INTO dim_date (date_id, date, day, month, year)
                        VALUES (?, ?, ?, ?, ?);
                    """, (next_date_id, date, day, month, year))
                    print(f"Inserted date_id={next_date_id} for date {date} into dim_date.")


        # Fetch and display the contents of dim_date
        results = conn.execute("SELECT * FROM dim_date LIMIT 2;").fetchall()
        print("Dimension dim_date created. Example rows:")
        for row in results:
            print(row)

        # 4. INSERTING DATA INTO dim_time
        for table in tables:
            table_name = table[0]
            if table_name[0].isdigit() and table_name.startswith("2"):  
                # Extract distinct times (hour, minute, second) from the TimeStamp column
                query = f"""
                    SELECT DISTINCT 
                        CAST(TimeStamp AS TIME) AS time,
                        EXTRACT(HOUR FROM CAST(TimeStamp AS TIMESTAMP)) AS hour,
                        EXTRACT(MINUTE FROM CAST(TimeStamp AS TIMESTAMP)) AS minute,
                        EXTRACT(SECOND FROM CAST(TimeStamp AS TIMESTAMP)) AS second
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
                    #print(f"Inserted time_id={next_time_id} for time '{time}' into dim_time.") #skipping the print because this insertion already makes the query slow



        # Fetch and display the contents of dim_time
        results = conn.execute("SELECT * FROM dim_time LIMIT 2;").fetchall()
        print("Dimension dim_time created. Example rows:")
        for row in results:
            print(row)

        #5. INSERTING DATA INTO dim_operator
        for table in tables:
            table_name = table[0]
            if table_name[0].isdigit() and table_name.startswith("2"):  
                # Extract distinct operators from the table
                query = f"""
                    SELECT DISTINCT "Operator" AS operator_name
                    FROM "{table_name}"
                    WHERE "Operator" IS NOT NULL
                """
                operator_rows = conn.execute(query).fetchall()

                for operator_row in operator_rows:
                    operator_name = operator_row[0]  # Extract operator_name from the row
                    operator_level = "medium"  # Operator level is always "medium" !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

                    # Check if the operator already exists in dim_operator
                    existing_operator = conn.execute("""
                        SELECT 1 FROM dim_operator WHERE operator_name = ?;
                    """, (operator_name,)).fetchone()

                    if existing_operator:  # Skip if the operator already exists
                        continue

                    # Determine the next available operator_id
                    max_operator_id = conn.execute(
                        "SELECT COALESCE(MAX(operator_id), 0) FROM dim_operator;"
                    ).fetchone()[0]
                    next_operator_id = max_operator_id + 1

                    # Insert into dim_operator using parameterized query
                    conn.execute("""
                        INSERT INTO dim_operator (operator_id, operator_name, operator_level)
                        VALUES (?, ?, ?);
                    """, (next_operator_id, operator_name, operator_level))
                    print(f"Inserted operator_id={next_operator_id} for operator {operator_name} into dim_operator.")

        # Fetch and display the contents of dim_operator
        results = conn.execute("SELECT * FROM dim_operator LIMIT 2;").fetchall()
        print("Dimension dim_operator created. Example rows:")
        for row in results:
            print(row)

        #6. INSERTING DATA INTO dim_electrode_material
        for table in tables:
            table_name = table[0]
            if table_name[0].isdigit() and table_name.startswith("2"):
                # Extract distinct electrode materials from the "Electrodes" column!!!!!!!!!!!!!!!!!!!!!!!!!
                query = f"""
                    SELECT DISTINCT "Electrodes" AS electrode_material
                    FROM "{table_name}"
                    WHERE "Electrodes" IS NOT NULL
                """
                electrode_materials = conn.execute(query).fetchall()

                for electrode_row in electrode_materials:
                    electrode_material = electrode_row[0]  # Extract the material value

                    # Check if the electrode_material already exists in dim_electrode_material
                    existing_material = conn.execute("""
                        SELECT 1 FROM dim_electrode_material WHERE electrode_material = ?;
                    """, (electrode_material,)).fetchone()

                    if existing_material:  # Skip if the material already exists
                        continue

                    # Determine the next available electrode_material_id
                    max_electrode_material_id = conn.execute(
                        "SELECT COALESCE(MAX(electrode_material_id), 0) FROM dim_electrode_material;"
                    ).fetchone()[0]
                    next_electrode_material_id = max_electrode_material_id + 1

                    # Insert into dim_electrode_material using parameterized query
                    conn.execute("""
                        INSERT INTO dim_electrode_material (electrode_material_id, electrode_material)
                        VALUES (?, ?);
                    """, (next_electrode_material_id, electrode_material))
                    print(f"Inserted electrode_material_id={next_electrode_material_id} for electrode_material '{electrode_material}' into dim_electrode_material.")

        # Fetch and display the contents of dim_electrode_material
        results = conn.execute("SELECT * FROM dim_electrode_material LIMIT 2;").fetchall()
        print("Dimension dim_electrode_material created. Example rows:")
        for row in results:
            print(row)


        # 7. INSERTING DATA INTO dim_dat_metadata (ONLY FOR MACHINE_2)
        for table in tables:
            table_name = table[0]
            if table_name[0].isdigit() and table_name.startswith("2"): #We are inserting into dim_dat_metadata only the data we got from machine_2
                # Extract distinct metadata rows from the table
                query = f"""
                    SELECT DISTINCT 
                        "Diaphragm" AS diaphragm,
                        "Thikness" AS thickness,
                        "Porosity" AS porosity,
                        "Electrolyte" AS electrolyte,
                        "Electrodes" AS electrodes,
                        CAST("Electrodes Area [cm²]" AS NUMERIC(10,3)) AS electrodes_area,
                        "Current Density Mode" AS current_density_mode,
                        CASE 
                            WHEN "Value [mA/cm²]" = 'NAN' THEN NULL
                            ELSE CAST("Value [mA/cm²]" AS NUMERIC(10,2))
                        END AS current_density_value,
                        "Pump Speed Mode" AS pump_speed_mode,
                        CASE 
                            WHEN "Value [ml/min]" = 'NAN' THEN NULL
                            ELSE CAST("Value [ml/min]" AS NUMERIC(10,2))
                        END AS pump_speed_value,
                        CASE 
                            WHEN "Experiment Running Time [h]" = 'NAN' THEN NULL
                            ELSE CAST("Experiment Running Time [h]" AS NUMERIC(10,2))
                        END AS experiment_running_time
                    FROM "{table_name}"
                """
                metadata_rows = conn.execute(query).fetchall()


                for metadata_row in metadata_rows:
                    # Check if the metadata already exists in dim_dat_metadata (used IS NOT DISTINCT FROM instead of "=" for comparing columns with potential NULL values)
                    existing_metadata = conn.execute("""
                        SELECT 1 FROM dim_dat_metadata
                        WHERE diaphragm = ? AND thickness = ? AND porosity = ? AND electrolyte = ? AND
                            electrodes = ? AND electrodes_area = ? AND current_density_mode = ? AND
                            current_density_value IS NOT DISTINCT FROM ? AND pump_speed_mode = ? AND
                            pump_speed_value IS NOT DISTINCT FROM ? AND experiment_running_time IS NOT DISTINCT FROM ?;
                    """, metadata_row).fetchone()

                    if existing_metadata:  # Skip if the metadata already exists
                        continue

                    # Determine the next available dat_metadata_id
                    max_metadata_id = conn.execute(
                        "SELECT COALESCE(MAX(dat_metadata_id), 0) FROM dim_dat_metadata;"
                    ).fetchone()[0]
                    next_metadata_id = max_metadata_id + 1

                    # Insert into dim_dat_metadata using parameterized query
                    conn.execute("""
                        INSERT INTO dim_dat_metadata (
                            dat_metadata_id, diaphragm, thickness, porosity, electrolyte, electrodes, electrodes_area,
                            current_density_mode, current_density_value, pump_speed_mode, pump_speed_value,
                            experiment_running_time
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """, (next_metadata_id, *metadata_row))
                    print(f"Inserted dat_metadata_id={next_metadata_id} for metadata {metadata_row} into dim_dat_metadata.")

        # Fetch and display the contents of dim_dat_metadata
        results = conn.execute("SELECT * FROM dim_dat_metadata LIMIT 2;").fetchall()
        print("Dimension dim_dat_metadata created. Example rows:")
        for row in results:
            print(row)

        #8. INSERT DATA INTO fact_dat_measurements (ONLY FOR MACHINE_2)
        for table in tables:
            table_name = table[0]
            if table_name[0].isdigit() and table_name.startswith("2008"): #NB! SIIN KIIRUSE PÄRAST LOEME SISSE AINULT 1 FAILI INFO
            #if table_name[0].isdigit() and table_name.startswith("2"):
                # Query to fetch all rows from the table
                query = f"""
                    SELECT 
                        "TimeStamp" AS timestamp,
                        CASE 
                            WHEN "Voltage [V]" = 'NAN' OR "Voltage [V]" IS NULL THEN NULL
                            ELSE CAST("Voltage [V]" AS NUMERIC(10,4))
                        END AS voltage_v,
                        CASE 
                            WHEN "H2 Flow" = 'NAN' OR "H2 Flow" IS NULL THEN NULL
                            ELSE CAST("H2 Flow" AS NUMERIC(10,4))
                        END AS h2_flow,
                        CASE 
                            WHEN "O2 Flow" = 'NAN' OR "O2 Flow" IS NULL THEN NULL
                            ELSE CAST("O2 Flow" AS NUMERIC(10,4))
                        END AS o2_flow,
                        CASE 
                            WHEN "H2 Purity [%]" = 'NAN' OR "H2 Purity [%]" IS NULL THEN NULL
                            ELSE CAST("H2 Purity [%]" AS NUMERIC(10,4))
                        END AS h2_purity_percent,
                        CASE 
                            WHEN "O2 Purity [%]" = 'NAN' OR "O2 Purity [%]" IS NULL THEN NULL
                            ELSE CAST("O2 Purity [%]" AS NUMERIC(10,4))
                        END AS o2_purity_percent,
                        CASE 
                            WHEN "Current [A]" = 'NAN' OR "Current [A]" IS NULL THEN NULL
                            ELSE CAST("Current [A]" AS NUMERIC(10,4))
                        END AS current_a,
                        CASE 
                            WHEN "H2 Pressure [bar]" = 'NAN' OR "H2 Pressure [bar]" IS NULL THEN NULL
                            ELSE CAST("H2 Pressure [bar]" AS NUMERIC(10,4))
                        END AS h2_pressure_bar,
                        CASE 
                            WHEN "O2 Pressure [bar]" = 'NAN' OR "O2 Pressure [bar]" IS NULL THEN NULL
                            ELSE CAST("O2 Pressure [bar]" AS NUMERIC(10,4))
                        END AS o2_pressure_bar,
                        CASE 
                            WHEN "SetPressure [bar]" = 'NAN' OR "SetPressure [bar]" IS NULL THEN NULL
                            ELSE CAST("SetPressure [bar]" AS NUMERIC(10,4))
                        END AS set_pressure_bar,
                        CASE 
                            WHEN "Cathode Temp [°C]" = 'NAN' OR "Cathode Temp [°C]" IS NULL THEN NULL
                            ELSE CAST("Cathode Temp [°C]" AS NUMERIC(10,4))
                        END AS cathode_temp_c,
                        CASE 
                            WHEN "Anode Temp [°C]" = 'NAN' OR "Anode Temp [°C]" IS NULL THEN NULL
                            ELSE CAST("Anode Temp [°C]" AS NUMERIC(10,4))
                        END AS anode_temp_c,
                        CASE 
                            WHEN "Cell Inlet Temp [°C]" = 'NAN' OR "Cell Inlet Temp [°C]" IS NULL THEN NULL
                            ELSE CAST("Cell Inlet Temp [°C]" AS NUMERIC(10,4))
                        END AS cell_inlet_temp_c,
                        CASE 
                            WHEN "[H2 Level [%]" = 'NAN' OR "[H2 Level [%]" IS NULL THEN NULL
                            ELSE CAST("[H2 Level [%]" AS NUMERIC(10,4))
                        END AS h2_level_percent,
                        CASE 
                            WHEN "O2 Level [%]" = 'NAN' OR "O2 Level [%]" IS NULL THEN NULL
                            ELSE CAST("O2 Level [%]" AS NUMERIC(10,4))
                        END AS o2_level_percent,
                        "Operator" AS operator_name,
                        "Electrodes" AS electrode_material,
                        CAST(TimeStamp AS DATE) AS date,
                        CAST(TimeStamp AS TIME) AS time
                    FROM "{table_name}"
                """
                rows = conn.execute(query).fetchall()

                # Fetch starting fact_dat_measurement_id
                max_fact_id = conn.execute(
                    "SELECT COALESCE(MAX(fact_dat_measurement_id), 0) FROM fact_dat_measurements;"
                ).fetchone()[0]

                # Batch insert setup
                batch_size = 500
                batch = []

                for row in rows:
                    # Increment fact_dat_measurement_id
                    max_fact_id += 1

                    # Extract row data
                    (timestamp, voltage_v, h2_flow, o2_flow, h2_purity_percent, o2_purity_percent, current_a,
                    h2_pressure_bar, o2_pressure_bar, set_pressure_bar, cathode_temp_c, anode_temp_c,
                    cell_inlet_temp_c, h2_level_percent, o2_level_percent, operator_name, electrode_material, date, time) = row

                    # Resolve IDs for dimensions
                    machine_id = conn.execute("SELECT machine_id FROM dim_machine WHERE machine_name = ?", 
                                            (f"machine_{table_name[0]}",)).fetchone()[0]
                    measurement_id = conn.execute("SELECT measurement_id FROM dim_measurement WHERE measurement_name = ?", 
                                                (table_name.split("_")[0],)).fetchone()[0]
                    operator_id = conn.execute("SELECT operator_id FROM dim_operator WHERE operator_name = ?", 
                                                (operator_name,)).fetchone()
                    operator_id = operator_id[0] if operator_id else None
                    date_id = conn.execute("SELECT date_id FROM dim_date WHERE date = ?", (date,)).fetchone()[0]
                    time_id = conn.execute("SELECT time_id FROM dim_time WHERE time = ?", (time,)).fetchone()[0]
                    electrode_material_id = conn.execute(
                        "SELECT electrode_material_id FROM dim_electrode_material WHERE electrode_material = ?", 
                        (electrode_material,)).fetchone()[0]
                    dat_metadata_id = conn.execute(
                        "SELECT dat_metadata_id FROM dim_dat_metadata WHERE electrodes = ?", 
                        (electrode_material,)).fetchone()
                    dat_metadata_id = dat_metadata_id[0] if dat_metadata_id else None

                    # Append the row to the batch
                    batch.append((
                        max_fact_id, machine_id, measurement_id, timestamp, date_id, time_id, 
                        electrode_material_id, dat_metadata_id, voltage_v, current_a, h2_flow, o2_flow,
                        h2_purity_percent, o2_purity_percent, h2_pressure_bar, o2_pressure_bar,
                        h2_level_percent, o2_level_percent, set_pressure_bar, cathode_temp_c, 
                        anode_temp_c, cell_inlet_temp_c, operator_id
                    ))

                    # Insert batch when batch size is met
                    if len(batch) >= batch_size:
                        conn.executemany("""
                            INSERT INTO fact_dat_measurements (
                                fact_dat_measurement_id, machine_id, measurement_id, timestamp, date_id, time_id,
                                electrode_material_id, dat_metadata_id, voltage_v, current_a, h2_flow, o2_flow,
                                h2_purity_percent, o2_purity_percent, h2_pressure_bar, o2_pressure_bar, 
                                h2_level_percent, o2_level_percent, set_pressure_bar, cathode_temp_c, 
                                anode_temp_c, cell_inlet_temp_c, operator_id
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """, batch)
                        batch = []

                # Insert remaining rows in the batch
                if batch:
                    conn.executemany("""
                        INSERT INTO fact_dat_measurements (
                            fact_dat_measurement_id, machine_id, measurement_id, timestamp, date_id, time_id,
                            electrode_material_id, dat_metadata_id, voltage_v, current_a, h2_flow, o2_flow,
                            h2_purity_percent, o2_purity_percent, h2_pressure_bar, o2_pressure_bar, 
                            h2_level_percent, o2_level_percent, set_pressure_bar, cathode_temp_c, 
                            anode_temp_c, cell_inlet_temp_c, operator_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """, batch)


                print(f"Finished inserting rows from table {table_name} into fact_dat_measurements.")

        # Fetch and display the contents of fact_dat_measurements
        results = conn.execute("SELECT * FROM fact_dat_measurements LIMIT 10;").fetchall()
        print("Fact table fact_dat_measurements created. Example rows:")
        for row in results:
            print(row)
        print("Fact table fact_dat_measurements created. Example rows:")
        for row in results:
            print(f"""
                fact_dat_measurement_id: {row[0]}, machine_id: {row[1]}, measurement_id: {row[2]}, 
                timestamp: {row[3]}, date_id: {row[4]}, time_id: {row[5]}, 
                electrode_material_id: {row[6]}, dat_metadata_id: {row[7]}, voltage_v: {row[8]}, 
                current_a: {row[9]}, h2_flow: {row[10]}, o2_flow: {row[11]}, 
                h2_purity_percent: {row[12]}, o2_purity_percent: {row[13]}, 
                h2_pressure_bar: {row[14]}, o2_pressure_bar: {row[15]}, 
                h2_level_percent: {row[16]}, o2_level_percent: {row[17]}, 
                set_pressure_bar: {row[18]}, cathode_temp_c: {row[19]}, 
                anode_temp_c: {row[20]}, cell_inlet_temp_c: {row[21]}, operator_id: {row[22]}
            """)


  
    finally:
        conn.close()



def duckdb_to_star_schema_machine_2(**kwargs):
    """Create star schema and populate it with data from DuckDB."""
    create_star_schema(**kwargs)
    populate_star_schema(**kwargs)

"""
# Create the DAG
with DAG(
    'duckdb_to_star_schema',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['transform']
) as dag:

    duckdb_to_star_schema = PythonOperator(
        task_id='duckdb_to_star_schema',
        python_callable=duckdb_to_star_schema
    )
"""
def get_duckdb_to_star_schema_machine_2_task(dag):
    return PythonOperator(
        task_id='duckdb_to_star_schema_machine_2',
        python_callable=duckdb_to_star_schema_machine_2,
        dag=dag
    )