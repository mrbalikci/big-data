
- Min Voltages 

var df_min = df.select(col("reading_dt").as("reading_time"), col("minimum_voltage"), lit("v").as("reading_unit"), col("device_id"), lit("CT").as("state"), lit("smart_meter").as("device_type"), lit("voltage_phase_A").as("reading_type"), col("reading_dt"))

 var df_max = df.select(col("reading_dt").as("reading_time"), col("maximum_voltage"), lit("v").as("reading_unit"), col("device_id"), lit("CT").as("state"), lit("smart_meter").as("device_type"), lit("voltage_phase_A").as("reading_type"), col("reading_dt"))

 Format of the date year-mm-date