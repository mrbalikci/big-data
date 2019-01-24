import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window



val db = "avangrid_dev2"
    //spark.conf.set("spark.broadcast.compress", "true")
    //spark.conf.set("spark.shuffle.compress", "true")
    //spark.conf.set("spark.shuffle.spill.compress", "false")
    //spark.conf.set("spark.io.compression.codec", "lzf")
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.conf.set("hive.exec.max.dynamic.partitions.pernode","10000")
    spark.conf.set("hive.exec.max.dynamic.partitions","100000")
    spark.conf.set("hive.optimize.sort.dynamic.partition","true")
    spark.conf.set("spark.sql.crossJoin.enabled", "true")

val spark = SparkSession.builder.appName("appName").enableHiveSupport().getOrCreate


package org.avangrid.analytics.config
package org.avangrid.analytics.config

spark.conf.set("spark.sql.crossJoin.enabled", "true")


var db = "avangrid_dev2"

def get_ct_cis(ss: SparkSession): DataFrame={ss.table(db + ".ct_cis")}
get_ct_cis(spark).count

var df_cis = get_ct_cis(spark)
df_cis.show



// READ PROC DATE 

var df_proc_date = spark.sql("select current_date proc_date")

df_proc_date = spark.sql("select to_date('2018-06-01') proc_date")

df_proc_date = df_proc_date.select(
     |       date_format(df_proc_date("proc_date"),"YYYY").as("YEAR"),
     |       date_format(df_proc_date("proc_date"),"MM").as("MONTH"),
     |       date_format(df_proc_date("proc_date"),"dd").as("DAY"))


df_proc_date.show


// GET NOTIFICATION AND DF IT

def get_notification(ss: SparkSession): DataFrame = {
    ss.table(db + ".notification")
  }

var df_notifications = get_notification(spark)

df_notifications.show

// FILTER BY THIME 
// CROSS JOIN syntax error 
// To get rid of this error: do cross join enabled 

spark.conf.set("spark.sql.crossJoin.enabled", "true")
df_notifications = df_notifications.join(df_proc_date,
      df_notifications("notification_year") === df_proc_date("YEAR") &&
        df_notifications("notification_month") === df_proc_date("MONTH") &&
        df_notifications("notification_day") === df_proc_date("DAY"),
      "inner").select(
      df_notifications("notification_time"),
      df_notifications("notification_code"),
      df_notifications("notification_description"),
      df_notifications("notification_long_description"),
      df_notifications("device_id"),
      df_notifications("state"),
      df_notifications("device_type"),
      df_notifications("notification_type"),
      df_notifications("notification_year"),
      df_notifications("notification_month"),
      df_notifications("notification_day")
    )

df_notifications.count
df_notifications.show

// Filter by type
df_notifications = df_notifications.filter("notification_type='Alarm'")


    var df_start = df_notifications.join(df_conf, df_conf("state") === df_notifications("state")
      && df_conf("notification_start_message") === df_notifications("notification_description")
      && df_conf("device_type") === df_notifications("device_type")
      ,"inner").select(
      df_notifications("device_id"),
      df_notifications("notification_description").as("event_description"),
      df_conf("device_type"),
      df_conf("event_type"),
      df_notifications("notification_time").as("start_time"),
      df_notifications("state")).distinct

 //arrange the end events
    var df_end = df_notifications.join(df_conf, df_conf("state") === df_notifications("state")
      && df_conf("notification_end_message") === df_notifications("notification_description")
      && df_conf("device_type") === df_notifications("device_type")
      ,"inner").select(df_notifications("device_id"),
      df_notifications("notification_description").as("event_description"),
      df_conf("device_type"),
      df_conf("event_type"),
      df_notifications("notification_time").as("end_time"),
      df_notifications("state")
      ).distinct

    var df_event = df_start.join(df_end, df_start("device_id") === df_end("device_id")
      && df_start("event_type") === df_end("event_type")
      && df_start("device_type") === df_end("device_type")
      && df_start("state") === df_end("state")
      && df_end("end_time") > df_start("start_time")
      ,"left").select(df_start("device_id"),
      df_start("event_description"),
      df_start("device_type"),
      df_start("event_type"),
      df_start("start_time"),
      df_end("end_time"),
      df_start("state"))


      df_event = df_event.groupBy("event_type","start_time","device_id", "event_description","device_type","state").agg(min("end_time").as("end_time"))


   //keeping records without end time
    var df_single_start = df_event.filter("end_time is null")

    df_single_start = df_single_start.select(
      df_single_start("state"),
      df_single_start("device_id"),
      df_single_start("event_description"),
      df_single_start("device_type"),
      df_single_start("event_type"),
      df_single_start("start_time")
    )

 // Get rid of null values for 'end_time'
    df_event = df_event.filter("end_time is not null")

    df_event = df_event.withColumn("event_elapsed_time", abs(col("start_time").cast("long")-col("end_time").cast("long"))/60D)

var df_meter = get_smart_meter(spark)

df_meter = df_meter.join(df_proc_date,
    df_meter("snapshot_year") === df_proc_date("YEAR") &&
    df_meter("snapshot_month") === df_proc_date("MONTH") &&
    df_meter("snapshot_day") === df_proc_date("DAY"),
  "inner").select(
  df_meter("smart_meter_id"),
  df_meter("reference_voltage_a"),
  df_meter("reference_voltage_b"),
  df_meter("reference_voltage_c"),
  df_meter("snapshot_year"),
  df_meter("snapshot_month"),
  df_meter("snapshot_day")
).show()

df_meter.show

// GET SINGLE NOTIFICATIONS 
get_single_notifications(spark).show


// GET REFERENCE VOLTAGE 
df_event = df_event.join(df_meter,
       df_event("device_id") === df_meter("smart_meter_id")
       //&& df_meter("snapshot_year")
       //&& df_meter("snapshot_month")
       //&& df_meter("snapshot_day")
       , "inner"). select(
       df_event("event_type"),
       df_event("start_time"),
       df_event("device_id"),
       df_event("event_description"),
       df_event("device_type"),
       df_event("state"),
       df_event("end_time"),
       df_event("event_elapsed_time"),
       df_meter("smart_meter_id"),
       df_meter("reference_voltage_a"),
       df_meter("reference_voltage_b"),
       df_meter("reference_voltage_c")

       )

  // GET VOLTAGES 
