var df_out_of_voltage_ct = get_out_of_voltage_event(spark).filter("state='CT'")

<!-- scala> df_out_of_voltage_ct.printSchema
root
 |-- event_start_time: timestamp (nullable = true)
 |-- event_end_time: timestamp (nullable = true)
 |-- smart_meter_id: string (nullable = true)
 |-- event_elapsed_time: double (nullable = true)
 |-- event_description: string (nullable = true)
 |-- event_voltage_phase: string (nullable = true)
 |-- event_max_voltage: double (nullable = true)
 |-- event_min_voltage: double (nullable = true)
 |-- event_mean_voltage: double (nullable = true)
 |-- event_out_of_voltage_percentage: double (nullable = true)
 |-- voltage_severity: string (nullable = true)
 |-- time_severity: string (nullable = true)
 |-- overall_severity: string (nullable = true)
 |-- state: string (nullable = true)
 |-- event_type: string (nullable = true)
 |-- event_start_year: string (nullable = true)
 |-- event_start_month: string (nullable = true)
 |-- event_start_day: string (nullable = true) -->


<!-- 1)	The process will use the table OUT_OF_VOLTAGE_EVENT to group all the events by STATE, EVENT_START_DT, SMART_METER_ID and EVENT_VOLTAGE_PHASE.  -->
// FILTER OUT OF VOLTAGE EVENT TABLE BASED ON A DATE 
// DATE 06-10
var the_date = "2018-06-10"

df_test.filter("date < '2018-06-10'").filter("date >= '2018-06-03'")

df_test.groupBy(
    col("smart_meter_id")).agg(
        count(col("date"))).distinct

df_out_of_voltage_ct.filter("event_start_month='06'").filter("event_start_day <'10'").filter("event_start_day >='03'").groupBy(col("event_start_day")).agg(count(col("event_start_day"))).distinct.show

// TEST
var df_test = df_out_of_voltage_ct.withColumn("date", to_date("event_start_time"))

df_test.groupBy($"smart_meter_id", window($"date", "10080 minutes")).agg(count("smart_meter_id")).distinct.show


df_out_of_voltage_ct.groupBy($"smart_meter_id", window($"date", "10080 minutes")).agg(count("smart_meter_id")).distinct.show

df_test.groupBy($"smart_meter_id", window($"event_start_day", "7 days")).agg(count("smart_meter_id")).distinct.show(50,false)

df_test.groupBy($"smart_meter_id", window($"date", "10080 minutes")).agg(count("event_start_day")).distinct.show(50,false)

// ADD 7 DAYS PERSISTENCE FACTOR 

var df_test = df_out_of_voltage_ct.groupBy(
    col("state"),
    col("event_start_year"),
    col("event_start_month"),
    col("event_start_day"),
    col("smart_meter_id"),
    col("event_voltage_phase"),
    col("event_max_voltage"),
    col("event_min_voltage"),
    col("event_mean_voltage"),
    col("voltage_severity"),
    col("time_severity"),
    col("overall_severity"),
    col("event_start_year"),
    col("event_start_month"),
    col("event_start_day"),
    window(col("event_start_time"), "10080 minutes")).agg(
            count("smart_meter_id").as("persistence_factor"))

df_out_of_voltage_ct.groupBy("smart_meter_id", window(col("event_start_time"), "10080 minutes")).agg(count("smart_meter_id").as("persistence_factor"))

import org.apache.spark.sql.expressions.Window

// OTHER STEPS 
// Convert time_stamp to long
val ts = df_out_of_voltage_ct("event_start_time").cast("long")

// window of 604800 = 7 days 
val w = Window.partitionBy(df_out_of_voltage_ct("smart_meter_id").Between(-604800,0)
    
