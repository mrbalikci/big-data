// DATABASE
// TODO: parametrize
val db = "avangrid_dev." 

// STATE
var state = "NY"
//Filter to get only active reference voltages 
var conf_filter = "state='" + state + "' AND COALESCE(event_mapping_effective_date, CURRENT_TIMESTAMP) <= CURRENT_TIMESTAMP AND COALESCE(event_mapping_expiration_date, CURRENT_TIMESTAMP) >= CURRENT_TIMESTAMP"

// READ CONFIG FOR EVENT MAPPING
var df_conf = spark.sql("select * from " + db + "event_mapping")

// FILTER
df_conf = df_conf.filter(conf_filter)
df_conf = df_conf.filter("device_type='smart_meter'")
df_conf.alias("conf")

// READ NOTIFICATIONS 
var df_notifications = spark.sql("select * from avangrid_dev.notification")

// FILTER FOR NY
df_notifications = df_notifications.filter("state='NY'")
df_notifications = df_notifications.filter("notification_type='Alarm'")

// GET EVENT DATA  
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
                                                   df_start("event_description"),
                                                   df_start("device_type"),
                                                   df_start("event_type"),
                                                   df_start("start_time"),
                                                   df_end("end_time"))


// EVENT DF
df_event = df_event.groupBy("event_type","start_time","device_id", "event_description","device_type").agg(min("end_time").as("end_time"))


// GET RID OF NULL VALUES 
df_event = df_event.filter("end_time is not null")

// GET THE VOLTAGE READING DAT FOR NY 
var df_voltage = spark.sql("select * from avangrid_dev.reading")

df_voltage = df_voltage.filter("state='NY'")

// JOIN EVENT AND VOLTAGE
var df_join = df_event.join(df_voltage, 
df_voltage("device_id")===df_event("device_id") && 
df_voltage("reading_time") >= df_event("start_time") && 
df_voltage("reading_time") <= df_event("end_time") &&
df_voltage("reading_dt") >= to_date(df_event("start_time")) &&
df_voltage("reading_dt") <= to_date(df_event("end_time"))
, "left"
).select(
df_event("device_id"), 
df_voltage("reading_time"), 
df_event("start_time"), 
df_event("end_time"),
df_event("event_description"),
df_event("device_type"),
df_event("event_type"), 
df_voltage("reading_value")
).toDF(
"device_id",
"reading_time",
"start_time",
"end_time",
"event_description",
"device_type",
"event_type",
"reading_value")

// CASE ONE WHERE READING VALUES NOT NULL
var df_case_one = df_join.filter("reading_value is not null")

// GET MAX, MIN, AND MEAN FOR CASE ONE 
df_case_one = df_case_one.groupBy("device_id", "start_time", "end_time", "reading_time", "event_description", "device_type", "event_type", "reading_value").agg(max("reading_value").as("max"), min("reading_value").as("min"), mean("reading_value").as("mean"))

// CASE TWO DEPENDENCIES 
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

// CASE TWO WHERE READING VALUES NULL 
var df_case_two = df_join.filter("reading_value is null").select(
col("device_id"),
col("reading_time"),
col("start_time"),
col("end_time"),
col("event_description"),
col("device_type"),
col("event_type"), 
col("reading_value")
)

// JOIN CASE TWO AND VOLTAGE READINGS 
df_case_two = df_case_two.join(df_voltage, df_voltage("device_id")===df_case_two("device_id") &&
                                 df_voltage("reading_time") >= ((df_case_two("start_time").cast("long") - 300).cast("Timestamp")) &&
                                 df_voltage("reading_time") <= ((df_case_two("start_time").cast("long") + 300).cast("Timestamp")), 
                                 "inner").select (
                                 df_case_two("device_id"),
                                 df_case_two("start_time"),
                                 df_case_two("end_time"),
                                 df_voltage("reading_time"),
                                 df_case_two("event_description"),
                                 df_case_two("device_type"),
                                 df_case_two("event_type"),
                                 df_voltage("reading_value")
).withColumn("diference",abs(df_case_two("start_time").cast("long") - df_voltage("reading_time").cast("long"))
).withColumn("rn",row_number().over(Window.partitionBy("device_id","start_time").orderBy("diference")))

// DROP LAST TWO COLUMNS THAT HELPED US TO GET THE LOGIC 
df_case_two = df_case_two.filter("rn=1").drop("rn").drop("diference")

// FINAL CASE TWO 
df_case_two = df_case_two.groupBy("device_id", "start_time", "end_time", "reading_time", "event_description", "device_type", "event_type", "reading_value").agg(max("reading_value").as("max"), min("reading_value").as("min"), mean("reading_value").as("mean"))

// CONCATENATE ALL THE CASES
var df_event_ny = df_case_one.unionAll(df_case_two)

// GET THE EVENT ELAPSED TIME
df_event_ny = df_event_ny.withColumn("event_elapsed_time", abs(($"start_time").cast("long")-($"end_time").cast("long"))/60D)

// GET THE SEVERITY LEVELS 
var ind = spark.sql("select * from avangrid_dev.indicator_severity_levels where state = 'NY' and indicator_name='TIME_SEVERITY'")

// APPLY THE INDICATOR 
df_event_ny = df_event_ny.join(ind, df_event_ny("event_elapsed_time") > ind("indicator_lower_bound") && df_event_ny("event_elapsed_time") <= ind("indicator_higher_bound"), "inner")

// GET THE SMART METER INFO FOR ID AND REFERENCE VOLTAGES
var df_meter = spark.sql("select * from avangrid_dev.ny_smart_meter_configuration")

df_meter = df_meter.select(col("servicepointid"), lit(240).as("reference_voltage"))

// JOIN EVENT AND SMART METER DF
var df_event_final = df_event_ny.join(df_meter, df_meter("servicepointid") === df_event_ny("device_id"), "inner").select(df_event_ny("device_id"),
                                    df_event_ny("start_time"), 
                                    df_event_ny("end_time"), 
                                    df_event_ny("event_type"), 
                                    df_event_ny("event_description"), 
                                    df_event_ny("device_type"),
                                    df_event_ny("reading_value"),
                                    df_event_ny("event_elapsed_time"),
                                    df_event_ny("max"),
                                    df_event_ny("min"),
                                    df_event_ny("mean"),
                                    df_meter("reference_voltage"),
                                    df_event_ny("indicator_severity").as("indicator_severity_time"))

// CALCULATE THE PRECENT 
var df_event_master = df_event_final.withColumn("percent", abs(col("reading_value")-col("reference_voltage"))/col("reference_voltage"))

// GET VOLTAGE SEVERITY 
var ind2 = spark.sql("select * from avangrid_dev.indicator_severity_levels where state = 'NY' and indicator_name='VOLTAGE_SEVERITY'")

// APPLY VOLTAGE SEVERITY INDICATOR 
df_event_master = df_event_master.join(ind2, df_event_master("percent") > ind2("indicator_lower_bound") && df_event_master("percent") <= ind2("indicator_higher_bound"), "inner")

// GET THE FINAL EVENT 
var df_final_master = df_event_master.select(
                df_event_master("device_id"),
                df_event_master("start_time"),
                df_event_master("end_time"),
                df_event_master("event_type"),
                df_event_master("event_description"),
                df_event_master("device_type"),
                df_event_master("reading_value"),
                df_event_master("event_elapsed_time"),
                df_event_master("max"),
                df_event_master("min"),
                df_event_master("mean"),
                df_event_master("reference_voltage"),
                df_event_master("indicator_severity_time"),
                df_event_master("percent"),
                df_event_master("indicator_severity").as("indicator_severity_voltage")
)

// GET OVERALL SEVERITY 
var ind3 = spark.sql("select * from avangrid_dev.overall_out_of_voltage_severity_levels where state='NY'")

// APPLY IT TO MASTER EVENT 
df_final_master = df_final_master.join(ind3, df_final_master("indicator_severity_time") === ind3("time_severity") && df_final_master("indicator_severity_voltage") === ind3("voltage_severity"), "inner")


// GET THE FINAL EVENT BEFORE APPENDING TO THE HIVE TABLE
var df_final = df_final_master.select(
                col("start_time").as("event_start_time"),
                col("end_time").as("event_end_time"),
                col("device_id").as("smart_meter_id"),
                col("event_elapsed_time"),
                col("event_description"),
                lit("Phase A").as("event_voltage_phase"),
                col("max").as("event_max_voltage"),
                col("min").as("event_min_voltage"),
                col("mean").as("event_mean_voltage"),
                col("percent").as("event_out_of_voltage_percentage"),
                col("indicator_severity_voltage").as("voltage_severity"),
                col("indicator_severity_time").as("time_severity"),
                col("overall_out_of_voltage_severity").as("overall_severity"),
                col("state"),
                col("event_type"),
                to_date(col("start_time")).as("event_start_dt")
)

