// DATABASE
// TODO: parametrize
val db = "avangrid_dev." 

// STATE
var state = "CT"
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


// EVENT 
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


df_event = df_event.groupBy("event_type","start_time","device_id", "event_description","device_type").agg(min("end_time").as("end_time"))


// Get rid of null values for 'end_time'
df_event = df_event.filter("end_time is not null")

// Read voltages 

var df_voltage = spark.sql("select * from avangrid_dev.reading")

df_voltage = df_voltage.filter("state='CT'")

df_voltage =  df_voltage.filter("reading_time='2018-06-01'")


df_voltage = df_voltage.groupBy(to_date(col("reading_time")).as("reading_date"),col("device_id")).agg(max("reading_value").as("maximum"), min("reading_value").as("minimum"))

df_voltage = df_voltage.withColumn("mean", (($"maximum" + $"minimum")/2))


// Join event with voltage
df_event = df_event.join(df_voltage, 
                df_voltage("device_id") === df_event("device_id") 
                && df_voltage("reading_date") === to_date(df_event("start_time")), "inner").select(df_event("device_id"), 
                        df_event("start_time"), 
                        df_event("end_time"), 
                        df_event("event_type"),
                        df_event("event_description"),
                        df_event("device_type"),
                        df_voltage("maximum"),
                        df_voltage("minimum"),
                        df_voltage("mean"),
                        when(df_event("event_type") === "Over Voltage", df_voltage("maximum")).otherwise(df_voltage("minimum")).as("voltage")
                        )

df_event = df_event.withColumn("event_elapsed_time", abs(($"start_time").cast("long")-($"end_time").cast("long"))/60D)

// Get the severity levels 
var ind = spark.sql("select * from avangrid_dev.indicator_severity_levels where state = 'CT' and indicator_name='TIME_SEVERITY'")

// Apply the indicator severity to time lapse
df_event = df_event.join(ind, df_event("event_elapsed_time") > ind("indicator_lower_bound") && df_event("event_elapsed_time") <= ind("indicator_higher_bound"), "inner")

// Get the Smart Meter IDs
var df_meter = spark.sql("select * from avangrid_dev2.smart_meter")

// See the data 
df_meter.select("reference_voltage_a").show()

df_meter.select("smart_meter_id", "reference_voltage_a").show()

df_meter = df_meter.select("smart_meter_id", "reference_voltage_a")

var df_event_final = df_event.join(df_meter,df_meter("smart_meter_id") === df_event("device_id"), "inner").select(df_event("device_id"), 

                                    df_event("start_time"), 
                                    df_event("end_time"), 
                                    df_event("event_type"), 
                                    df_event("event_description"), 
                                    df_event("device_type"), 
                                    df_event("voltage"),
                                    df_event("event_elapsed_time"),
                                    df_voltage("maximum"),
                                    df_voltage("minimum"),
                                    df_voltage("mean"),
                                    df_meter("reference_voltage_a"),
                                    df_event("indicator_severity").as("indicator_severity_time"))

var df_event_master = df_event_final.withColumn("percent", abs(col("voltage")-col("reference_voltage_a"))/col("reference_voltage_a"))

var ind2 = spark.sql("select * from avangrid_dev.indicator_severity_levels where state = 'CT' and indicator_name='TIME_SEVERITY'")

df_event_master = df_event_master.join(ind2, df_event_master("percent") > ind2("indicator_lower_bound") && df_event_master("percent") <= ind2("indicator_higher_bound"), "inner")

var df_final_master = df_event_master.select(
                col("device_id"),
                col("start_time"),
                col("end_time"),
                col("event_type"),
                col("event_description"),
                col("device_type"),
                col("voltage"),
                col("event_elapsed_time"),
                df_voltage("maximum"),
                df_voltage("minimum"),
                df_voltage("mean"),
                col("reference_voltage_a").as("reference_voltage"),
                col("indicator_severity_time"),
                col("percent"),
                col("indicator_severity").as("indicator_severity_voltage")
)

var ind3 = spark.sql("select * from avangrid_dev.overall_out_of_voltage_severity_levels where state='CT'")

df_final_master = df_final_master.join(ind3, df_final_master("indicator_severity_time") === ind3("time_severity") && df_final_master("indicator_severity_voltage") === ind3("voltage_severity"), "inner")


var df_final = df_final_master.select(
                col("start_time").as("event_start_time"),
                col("end_time").as("event_end_time"),
                col("device_id").as("smart_meter_id"),
                col("event_elapsed_time"),
                col("event_description"),
                lit("Phase A").as("event_voltage_phase"),
                col("maximum").as("event_max_voltage"),
                col("minimum").as("event_min_voltage"),
                col("mean").as("event_mean_voltage"),
                col("percent").as("event_out_of_voltage_percentage"),
                col("indicator_severity_voltage").as("voltage_severity"),
                col("indicator_severity_time").as("time_severity"),
                col("overall_out_of_voltage_severity").as("overall_severity"),
                col("state"),
                col("event_type"),
                to_date(col("start_time")).as("event_start_dt")
)