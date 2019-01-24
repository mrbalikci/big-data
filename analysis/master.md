val db = "avangrid_dev." // database name TODO: parametrize
var state = "CT" //STATE 
//Filter to get only active reference voltages 
var conf_filter = "state='" + state + "' AND COALESCE(event_mapping_effective_date, CURRENT_TIMESTAMP) <= CURRENT_TIMESTAMP AND COALESCE(event_mapping_expiration_date, CURRENT_TIMESTAMP) >= CURRENT_TIMESTAMP"

var df_conf = spark.sql("select * from " + db + "event_mapping")

df_conf = df_conf.filter(conf_filter)
df_conf = df_conf.filter("device_type='smart_meter'")
df_conf.alias("conf")

var df_notifications = spark.sql("select * from avangrid_dev.notification")

//CT
df_notifications = df_notifications.filter("state='CT'")
df_notifications = df_notifications.filter("notification_dt='2018-06-01'")
df_notifications = df_notifications.filter("notification_type='Alarm'")


//arrange the start events     
var df_start = df_notifications.join(df_conf, df_conf("state") === df_notifications("state") 
               && df_conf("notification_start_message") === df_notifications("notification_description") 
               && df_conf("device_type") === df_notifications("device_type")
               ,"inner").select(df_notifications("device_id"),
                                df_notifications("notification_description").as("event_description"),
                                df_conf("device_type"),
                                df_conf("event_type"), 
                                df_notifications("notification_time").as("start_time")).distinct

var df_end = df_notifications.join(df_conf, df_conf("state") === df_notifications("state") 
               && df_conf("notification_end_message") === df_notifications("notification_description") 
               && df_conf("device_type") === df_notifications("device_type")
               ,"inner").select(df_notifications("device_id"),
                                df_notifications("notification_description").as("event_description"),
                                df_conf("device_type"),
                                df_conf("event_type"), 
                                df_notifications("notification_time").as("end_time")).distinct
               
var df_event = df_start.join(df_end, df_start("device_id") === df_end("device_id")
                                   && df_start("event_type") === df_end("event_type")
                                   && df_start("device_type") === df_end("device_type")
                                   && df_end("end_time") > df_start("start_time")
                                   ,"left").select(df_start("device_id"),
                                                   df_start("event_description")
                                                   df_start("device_type"),
                                                   df_start("event_type"),
                                                   df_start("start_time"),
                                                   df_end("end_time"))


df_event = df_event.groupBy("event_type","start_time","device_id").agg(min("end_time").as("end_time"))

df_event.show(10,false)

// Get rid of null values for 'end_time'
df_event = df_event.filter("end_time is not null")

// Read voltages 

var df_voltage = spark.sql("select * from avangrid_dev.reading")

df_voltage = df_voltage.filter("state='CT'")

df_voltage =  df_voltage.filter("reading_time='2018-06-01'")


df_voltage = df_voltage.groupBy(to_date(col("reading_time")).as("reading_date"),col("device_id")).agg(max("reading_value").as("maximum"), min("reading_value").as("minimum"))

df_voltage.show(10, false)

// Join event and voltages under df_event 

df_event = df_event.join(df_voltage, 
                df_voltage("device_id") === df_event("device_id") 
                && df_voltage("reading_date") === to_date(df_event("start_time")), "inner").select(df_event("device_id"), 
                        df_event("start_time"), 
                        df_event("end_time"), 
                        df_event("event_type"),
                        df_event("event_description"),
                        df_event("device_type"),
                        when(df_event("event_type") === "Over Voltage", df_voltage("maximum")).otherwise(df_voltage("minimum")).as("voltage")
                        )

 df_event = df_event.withColumn("event_elapsed_time", abs(($"start_time").cast("long")-($"end_time").cast("long"))/60D)

 // round the number to 0 decimal place
 df_event = df_event.select(col("device_id"), col("start_time"), col("end_time"), col("event_type"), col("voltage"), bround(df_event("event_elapsed_time"),0).as("time_diff"))

// Get the severity levels 

var ind = spark.sql("select * from avangrid_dev.indicator_severity_levels")

df_voltage_perc.join(ind, df_voltage_perc("percent") > ind("indicator_lower_bound") && df_voltage_perc("percent") <= ind("indicator_higher_bound"), "inner").filter("indicator_severity = 'MEDIUM'").show(100,false)


// CRITICALITIES FOR TIME 

df_event = df_event.withColumn("criticality1", when(col("time_diff").between(0.0,15.0),"LOW").otherwise(when(col("time_diff").between(15.0,60.0),"MEDIUM")))

df_event = df_event.withColumn("criticality2", when(col("time_diff").between(60.0,360.0),"HIGH").otherwise(when(col("time_diff").between(60.0,1.0E11),"VERY HIGH")))

df_event = df_event.withColumn("time_severity", when(col("time_diff").between(60.0,360.0),"HIGH").otherwise(when(col("time_diff").between(60.0,1.0E11),"VERY HIGH")))

// ADD SMART METER IDs FROM SMART METER TABLE 
// Get Smart Meter IDs 

var df_meter = spark.sql("select * from avangrid_dev.smart_meter")

// See the data 
df_meter.select("reference_voltage_a").show()

df_meter.select("smart_meter_id", "reference_voltage_a").show()

df_meter = df_meter.select("smart_meter_id", "reference_voltage_a")

// Join the tables as event master df 
var df_event_master = df_event.join(df_meter,df_meter("smart_meter_id") === df_event("device_id"), "inner").select(df_event("device_id"), df_event("start_time"), df_event("end_time"), df_event("event_type"), df_event("voltage"), df_meter("reference_voltage_a"), df_event("time_diff"), df_event("time_severity"))

// Find the difference between voltage and reference voltage 
df_event_master = df_event_master.withColumn("diff_voltages", abs(($"voltage")-($"reference_voltage_a")))


df_event_master = df_event_master.select(col("device_id"), col("start_time"), col("end_time"), col("event_type"), col("voltage"), col("reference_voltage_a"), col("time_diff"), col("time_severity"), bround(df_event_master("diff_voltages"),2).as("diff_in_voltage"))

// find the percent of the difference in voltage and reference and round to 2 digit 
df_event_master = df_event_master.withColumn("ratio_voltage", abs(($"diff_in_voltage")/($"reference_voltage_a")),2))

df_event_master = df_event_master.withColumn("voltage_critic1", when(col("ratio_voltage").between(0.0,0.05),"LOW").otherwise(when(col("ratio_voltage").between(0.05,0.1),"MEDIUM")))

df_event_master = df_event_master.withColumn("voltage_critic2", when(col("ratio_voltage").between(0.1,1.0),"HIGH"))

df_event_master = df_event_master.withColumn("voltage_severity", coalesce($"voltage_critic1", $"voltage_critic2")).drop("voltage_critic1").drop("voltage_critic2")

df_event_master.show()




