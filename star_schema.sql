CREATE TABLE IF NOT EXISTS "dim_machine" (
	"machine_id" integer NOT NULL,
	"machine_name" varchar(100) NOT NULL,
	"machine_type" varchar(50) NOT NULL,
	PRIMARY KEY ("machine_id")
);

CREATE TABLE IF NOT EXISTS "dim_operator" (
	"operator_id" integer NOT NULL,
	"operator_name" varchar(100) NOT NULL,
	"operator_level" varchar(50) NOT NULL,
	PRIMARY KEY ("operator_id")
);

CREATE TABLE IF NOT EXISTS "dim_timestamp" (
	"timestamp_id" integer NOT NULL,
	"timestamp" timestamp with time zone NOT NULL,
	PRIMARY KEY ("timestamp_id")
);

CREATE TABLE IF NOT EXISTS "dim_date" (
	"date_id" integer NOT NULL,
	"date" date NOT NULL,
	"day" bigint NOT NULL,
	"month" bigint NOT NULL,
	"year" bigint NOT NULL,
	PRIMARY KEY ("date_id")
);

CREATE TABLE IF NOT EXISTS "dim_time" (
	"time_id" integer NOT NULL,
	"hour" bigint NOT NULL,
	"minute" bigint NOT NULL,
	"second" bigint NOT NULL,
	PRIMARY KEY ("time_id")
);

CREATE TABLE IF NOT EXISTS "dim_electrode_material" (
	"electrode_material_id" integer NOT NULL,
	"electrode_material" varchar(100) NOT NULL,
	PRIMARY KEY ("electrode_material_id")
);

CREATE TABLE IF NOT EXISTS "dim_measurement" (
	"measurement_id" integer NOT NULL,
	"measurement_name" varchar(100) NOT NULL,
	"start_timestamp" timestamp with time zone NOT NULL,
	"finish_timestamp" timestamp with time zone NOT NULL,
	PRIMARY KEY ("measurement_id")
);

CREATE TABLE IF NOT EXISTS "dim_dat_metadata" (
	"dat_metadata_id" integer NOT NULL,
	"diaphragm" varchar(100) NOT NULL,
	"thickness" bigint NOT NULL,
	"porosity" bigint NOT NULL,
	"electrolyte" varchar(100) NOT NULL,
	"electrodes" varchar(100) NOT NULL,
	"electrodes_area" numeric(10,3) NOT NULL,
	"current_density_mode" varchar(50) NOT NULL,
	"current_density_value" numeric(10,2) NOT NULL,
	"pump_speed_mode" varchar(50) NOT NULL,
	"pump_speed_value" numeric(10,2) NOT NULL,
	"experiment_running_time" numeric(10,2) NOT NULL,
	PRIMARY KEY ("dat_metadata_id")
);

CREATE TABLE IF NOT EXISTS "fact_ndax_measurements" (
	"fact_ndax_measurement_id" integer NOT NULL,
	"measurement_id" bigint NOT NULL,
	"machine_id" bigint NOT NULL,
	"operator_id" bigint NOT NULL,
	"timestamp_id" bigint NOT NULL,
	"date_id" bigint NOT NULL,
	"time_id" bigint NOT NULL,
	"electrode_material_id" bigint NOT NULL,
	"timestamp" timestamp with time zone NOT NULL,
	"record_id" bigint NOT NULL,
	"cycle" bigint NOT NULL,
	"step_id" bigint NOT NULL,
	"step_name" varchar(50) NOT NULL,
	"time_in_step" numeric(18,8) NOT NULL,
	"voltage_v" numeric(18,8) NOT NULL,
	"current_ma" numeric(18,8) NOT NULL,
	"capacity_mah" numeric(18,8) NOT NULL,
	"energy_mwh" numeric(18,8) NOT NULL,
	PRIMARY KEY ("fact_ndax_measurement_id")
);

CREATE TABLE IF NOT EXISTS "fact_dat_measurements" (
	"fact_dat_measurement_id" integer NOT NULL,
	"measurement_id" bigint NOT NULL,
	"machine_id" bigint NOT NULL,
	"operator_id" bigint NOT NULL,
	"timestamp_id" bigint NOT NULL,
	"date_id" bigint NOT NULL,
	"time_id" bigint NOT NULL,
	"electrode_material_id" bigint NOT NULL,
	"dat_metadata_id" bigint NOT NULL,
	"timestamp" timestamp with time zone NOT NULL,
	"voltage_v" numeric(10,4) NOT NULL,
	"h2_flow" numeric(10,4) NOT NULL,
	"o2_flow" numeric(10,4) NOT NULL,
	"h2_purity_percent" numeric(10,4) NOT NULL,
	"o2_purity_percent" numeric(10,4) NOT NULL,
	"current_a" numeric(10,4) NOT NULL,
	"h2_pressure_bar" numeric(10,4) NOT NULL,
	"o2_pressure_bar" numeric(10,4) NOT NULL,
	"set_pressure_bar" numeric(10,4) NOT NULL,
	"cathode_temp_c" numeric(10,4) NOT NULL,
	"anode_temp_c" numeric(10,4) NOT NULL,
	"cell_inlet_temp_c" numeric(10,4) NOT NULL,
	"h2_level_percent" numeric(10,4) NOT NULL,
	"o2_level_percent" numeric(10,4) NOT NULL,
	PRIMARY KEY ("fact_dat_measurement_id")
);









ALTER TABLE "fact_ndax_measurements" ADD CONSTRAINT "fact_ndax_measurements_fk1" FOREIGN KEY ("measurement_id") REFERENCES "dim_measurement"("measurement_id");

ALTER TABLE "fact_ndax_measurements" ADD CONSTRAINT "fact_ndax_measurements_fk2" FOREIGN KEY ("machine_id") REFERENCES "dim_machine"("machine_id");

ALTER TABLE "fact_ndax_measurements" ADD CONSTRAINT "fact_ndax_measurements_fk3" FOREIGN KEY ("operator_id") REFERENCES "dim_operator"("operator_id");

ALTER TABLE "fact_ndax_measurements" ADD CONSTRAINT "fact_ndax_measurements_fk4" FOREIGN KEY ("timestamp_id") REFERENCES "dim_timestamp"("timestamp_id");

ALTER TABLE "fact_ndax_measurements" ADD CONSTRAINT "fact_ndax_measurements_fk5" FOREIGN KEY ("date_id") REFERENCES "dim_date"("date_id");

ALTER TABLE "fact_ndax_measurements" ADD CONSTRAINT "fact_ndax_measurements_fk6" FOREIGN KEY ("time_id") REFERENCES "dim_time"("time_id");

ALTER TABLE "fact_ndax_measurements" ADD CONSTRAINT "fact_ndax_measurements_fk7" FOREIGN KEY ("electrode_material_id") REFERENCES "dim_electrode_material"("electrode_material_id");
ALTER TABLE "fact_dat_measurements" ADD CONSTRAINT "fact_dat_measurements_fk1" FOREIGN KEY ("measurement_id") REFERENCES "dim_measurement"("measurement_id");

ALTER TABLE "fact_dat_measurements" ADD CONSTRAINT "fact_dat_measurements_fk2" FOREIGN KEY ("machine_id") REFERENCES "dim_machine"("machine_id");

ALTER TABLE "fact_dat_measurements" ADD CONSTRAINT "fact_dat_measurements_fk3" FOREIGN KEY ("operator_id") REFERENCES "dim_operator"("operator_id");

ALTER TABLE "fact_dat_measurements" ADD CONSTRAINT "fact_dat_measurements_fk4" FOREIGN KEY ("timestamp_id") REFERENCES "dim_timestamp"("timestamp_id");

ALTER TABLE "fact_dat_measurements" ADD CONSTRAINT "fact_dat_measurements_fk5" FOREIGN KEY ("date_id") REFERENCES "dim_date"("date_id");

ALTER TABLE "fact_dat_measurements" ADD CONSTRAINT "fact_dat_measurements_fk6" FOREIGN KEY ("time_id") REFERENCES "dim_time"("time_id");

ALTER TABLE "fact_dat_measurements" ADD CONSTRAINT "fact_dat_measurements_fk7" FOREIGN KEY ("electrode_material_id") REFERENCES "dim_electrode_material"("electrode_material_id");

ALTER TABLE "fact_dat_measurements" ADD CONSTRAINT "fact_dat_measurements_fk8" FOREIGN KEY ("dat_metadata_id") REFERENCES "dim_dat_metadata"("dat_metadata_id");