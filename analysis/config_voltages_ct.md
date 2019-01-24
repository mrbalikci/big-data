// Get Database
val db = "avangrid_dev." 

// Get State
var state = "CT" 

//Filter to get only active reference voltages 
var conf_filter = "state='" + state + "' AND COALESCE(event_mapping_effective_date, CURRENT_TIMESTAMP) <= CURRENT_TIMESTAMP AND COALESCE(event_mapping_expiration_date, CURRENT_TIMESTAMP) >= CURRENT_TIMESTAMP"

var df_conf = spark.sql("select * from " + db + "event_mapping")

df_conf = df_conf.filter(conf_filter)
df_conf = df_conf.filter("device_type='smart_meter'")
df_conf.alias("conf")

var df_voltage = spark.sql("select * from avangrid_dev.reading")

df_voltage = df_voltage.filter("state='CT'")

df_voltage =  df_voltage.filter("reading_time='2018-06-01'")


df_voltage = df_voltage.groupBy(to_date(col("reading_time")).as("reading_date"),col("device_id")).agg(
        max("reading_value").as("maximum"), 
        min("reading_value").as("minimum"))

<!-- var df_event_max = df_voltage.join(df_event, df_voltage("device_id") === df_event("device_id"), "inner").select(df_voltage("device_id"), df_event("event_type"), df_event("start_time"), df_event("end_time"), df_voltage("maximum")).distinct -->


df_event = df_event.join(df_voltage, 
                df_voltage("device_id") === df_event("device_id") 
                && df_voltage("reading_date") === to_date(df_event("start_time")), "inner").select(df_event("device_id"), 
                        df_event("start_time"), 
                        df_event("end_time"), 
                        df_event("event_type"), 
                        when(df_event("event_type") === "Over Voltage", df_voltage("maximum")).otherwise(df_voltage("minimum")).as("voltage")
                        )

 df_event = df_event.withColumn("diff in min", abs(($"start_time").cast("long")-($"end_time").cast("long"))/60D)

 // round the number to 0 decimal place
 df_event = df_event.select(col("device_id"), col("start_time"), col("end_time"), col("event_type"), col("voltage"), bround(df_event("diff in min"),0).as("time_diff"))

