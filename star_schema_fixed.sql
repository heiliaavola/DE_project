CREATE TABLE IF NOT EXISTS dim_machine (
    machine_id INTEGER NOT NULL PRIMARY KEY,
    machine_name VARCHAR(100) NOT NULL UNIQUE,
    machine_type VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_measurement (
    measurement_id INTEGER NOT NULL PRIMARY KEY,
    measurement_name VARCHAR(100) NOT NULL UNIQUE,
    start_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    finish_timestamp TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id INTEGER NOT NULL,
    date DATE NOT NULL UNIQUE,
    day BIGINT NOT NULL,
    month BIGINT NOT NULL,
    year BIGINT NOT NULL,
    PRIMARY KEY (date_id)
);

CREATE TABLE IF NOT EXISTS dim_time (
    time_id INTEGER NOT NULL,
    time TIME NOT NULL UNIQUE,
    hour BIGINT NOT NULL,
    minute BIGINT NOT NULL,
    second BIGINT NOT NULL,
    PRIMARY KEY (time_id)
);

CREATE TABLE IF NOT EXISTS dim_operator (
    operator_id INTEGER NOT NULL,
    operator_name VARCHAR(100) NOT NULL UNIQUE,
    operator_level VARCHAR(50) NOT NULL,
    PRIMARY KEY (operator_id)
);

CREATE TABLE IF NOT EXISTS dim_electrode_material (
    electrode_material_id INTEGER NOT NULL,
    electrode_material VARCHAR(100) NOT NULL UNIQUE,
    PRIMARY KEY (electrode_material_id)
);

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