// Get Smart Meter IDs 

var df_meter = spark.sql("select * from avangrid_dev.smart_meter")

// See the data 
df_meter.select("reference_voltage_a").show()

df_meter.select("smart_meter_id", "reference_voltage_a").show()

df_meter = df_meter.select("smart_meter_id", "reference_voltage_a")

// do joining on device_id, inner join as a new df 
var df_event_master = df_event.join(df_meter,df_meter("smart_meter_id") === df_event("device_id"), "inner").select(df_event("device_id"), df_event("start_time"), df_event("end_time"), df_event("event_type"), df_event("voltage"), df_meter("reference_voltage_a"), df_event("time_diff"), df_event("time_severity"))

// see the new data 
df_event_master.show(10)

df_event_master.count

df_event.count

df_event.show

// find the difference between voltage and reference voltage 
df_event_master = df_event_master.withColumn("diff_voltages", abs(($"voltage")-($"reference_voltage_a")))

// round the difference and select columns in a way 
df_event_master = df_event_master.select(col("device_id"), col("start_time"), col("end_time"), col("event_type"), col("voltage"), col("reference_voltage_a"), col("time_diff"), col("time_severity"), bround(df_event_master("diff_voltages"),2).as("diff_in_voltage"))


// find the percent of the difference in voltage and reference and round to 2 digit 
df_event_master = df_event_master.withColumn("diff_voltages", bround(abs(($"diff_in_voltage")/($"reference_voltage_a")),2))

// see the data 
df_event_master.select("diff_voltages").show
