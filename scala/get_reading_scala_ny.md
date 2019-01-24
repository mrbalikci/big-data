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


// FILTER BY THE DATE 
var df_proc_date = spark.sql("select current_date proc_date")

df_proc_date = spark.sql("select to_date('2018-10-19') proc_date")
// FOR NY OCT-01

df_proc_date = df_proc_date.select(
     |       date_format(df_proc_date("proc_date"),"YYYY").as("YEAR"),
     |       date_format(df_proc_date("proc_date"),"MM").as("MONTH"),
     |       date_format(df_proc_date("proc_date"),"dd").as("DAY"))

df_proc_date.show

var df_reading = get_reading(spark)
df_reading = df_reading.join(df_proc_date,
      df_reading("reading_year") === df_proc_date("YEAR") &&
        df_reading("reading_month") === df_proc_date("MONTH") &&
        df_reading("reading_day") === df_proc_date("DAY"),
      "inner").select(
          df_reading("reading_time"),
          df_reading("reading_value"),
          df_reading("reading_unit"),
          df_reading("device_id"),
          df_reading("state"),
          df_reading("device_type"),
          df_reading("reading_type"),
          df_reading("reading_year"),
          df_reading("reading_month"),
          df_reading("reading_day"),
          df_reading("device_type")
    )

df_reading.show()

 <!-- |-- state: string (nullable = true)
 |-- device_id: string (nullable = true)
 |-- Voltage Phase A: double (nullable = true)
 |-- Voltage Phase B: double (nullable = true)
 |-- Voltage Phase C: double (nullable = true) -->


var df_reading_ny = df_reading.join(df_meter, df_meter("smart_meter_id")===df_reading("device_id") && df_meter("state")===df_reading("state")
                                    ).select(
                                        df_reading("state"),
                                        df_reading("reading_time"),
                                        df_reading("device_type"),
                                        df_reading("device_id"),
                                        df_reading("reading_type"),
                                        df_reading("reading_year"),
                                        df_reading("reading_month"),
                                        df_reading("reading_value"),
                                        df_reading("reading_day"),
                                        df_meter("reference_voltage_a"),
                                        df_meter("reference_voltage_b"),
                                        df_meter("reference_voltage_c")
                                    )
<!-- 
df_reading_ny.printSchema
root
 |-- state: string (nullable = true)
 |-- reading_time: timestamp (nullable = true)
 |-- device_type: string (nullable = true)
 |-- device_id: string (nullable = true)
 |-- reading_type: string (nullable = true)
 |-- reading_year: string (nullable = true)
 |-- reading_month: string (nullable = true)
 |-- reading_value: double (nullable = true)
 |-- reading_day: string (nullable = true)
 |-- reference_voltage_a: double (nullable = true)
 |-- reference_voltage_b: double (nullable = true)
 |-- reference_voltage_c: double (nullable = true) -->

df_reading_ny = df_reading_ny.groupBy(
    col("device_id").as("smart_meter_id"),
    hour(col("reading_time")).as("hour"),
    col("state"),
    col("reading_year"),
    col("reading_month"), 
    col("reading_day"),
    df_reading_ny("reading_type"),
    df_reading_ny("reference_voltage_a"),
    df_reading_ny("reference_voltage_b"),
    df_reading_ny("reference_voltage_c")
    ).agg(
        max("reading_value").as("maximum"), 
        min("reading_value").as("minimum"), 
        mean("reading_value").as("mean"))

<!-- root
 |-- smart_meter_id: string (nullable = true)
 |-- hour: integer (nullable = true)
 |-- state: string (nullable = true)
 |-- reading_year: string (nullable = true)
 |-- reading_month: string (nullable = true)
 |-- reading_day: string (nullable = true)
 |-- reading_type: string (nullable = true)
 |-- reference_voltage_a: double (nullable = true)
 |-- reference_voltage_b: double (nullable = true)
 |-- reference_voltage_c: double (nullable = true)
 |-- maximum: double (nullable = true)
 |-- minimum: double (nullable = true)
 |-- mean: double (nullable = true) -->


var df_reading_ny_a = df_reading_ny.filter("reading_type='Voltage Phase A'")

 df_reading_ny_a = df_reading_ny_a.withColumn("average_percent_a",df_reading_ny_a.col("mean")/df_reading_ny_a.col("reference_voltage_a"))

df_reading_ny_a = df_reading_ny_a.withColumn("minimum_percent_a",df_reading_ny_a.col("minimum")/df_reading_ny_a.col("reference_voltage_a"))

df_reading_ny_a= df_reading_ny_a.withColumn("maximum_percent_a",df_reading_ny_a.col("maximum")/df_reading_ny_a.col("reference_voltage_a"))


// SELECT APPROPIRATE COLUMNS FOR PHASE A
df_reading_ny_a = df_reading_ny_a.select(
    col("smart_meter_id"), 
    col("hour").as("reading_hour"), 
    col("mean").as("reading_average_a"), 
    col("maximum").as("reading_maximum_a"), 
    col("minimum").as("reading_minimum_a"), 
    col("average_percent_a"), 
    col("maximum_percent_a"), 
    col("minimum_percent_a"),
    col("state"),
    col("reading_year"),
    col("reading_month"), 
    col("reading_day"))


// PHASE B 
var df_reading_ny_b = df_reading_ny.filter("reading_type='Voltage Phase B'")

df_reading_ny_b = df_reading_ny_b.withColumn("average_percent_b",df_reading_ny_b.col("mean")/df_reading_ny_b.col("reference_voltage_b"))

df_reading_ny_b = df_reading_ny_b.withColumn("minimum_percent_b",df_reading_ny_b.col("minimum")/df_reading_ny_b.col("reference_voltage_b"))

df_reading_ny_b= df_reading_ny_b.withColumn("maximum_percent_b",df_reading_ny_b.col("maximum")/df_reading_ny_b.col("reference_voltage_b"))

// SELECT APPROPIRATE COLUMNS FOR PHASE B
df_reading_ny_b = df_reading_ny_b.select(
    col("smart_meter_id"), 
    col("hour").as("reading_hour"), 
    col("mean").as("reading_average_b"), 
    col("maximum").as("reading_maximum_b"), 
    col("minimum").as("reading_minimum_b"), 
    col("average_percent_b"), 
    col("maximum_percent_b"), 
    col("minimum_percent_b"),
    col("state"),
    col("reading_year"),
    col("reading_month"), 
    col("reading_day"))


// PHASE C 
var df_reading_ny_c = df_reading_ny.filter("reading_type='Voltage Phase C'")

df_reading_ny_c = df_reading_ny_c.withColumn("average_percent_c",df_reading_ny_c.col("mean")/df_reading_ny_c.col("reference_voltage_c"))

df_reading_ny_c = df_reading_ny_c.withColumn("minimum_percent_c",df_reading_ny_c.col("minimum")/df_reading_ny_c.col("reference_voltage_c"))

df_reading_ny_c= df_reading_ny_c.withColumn("maximum_percent_c",df_reading_ny_c.col("maximum")/df_reading_ny_c.col("reference_voltage_c"))

// SELECT APPROPIRATE COLUMNS FOR PHASE C
df_reading_ny_c = df_reading_ny_c.select(
    col("smart_meter_id"), 
    col("hour").as("reading_hour"), 
    col("mean").as("reading_average_c"), 
    col("maximum").as("reading_maximum_c"), 
    col("minimum").as("reading_minimum_c"), 
    col("average_percent_c"), 
    col("maximum_percent_c"), 
    col("minimum_percent_c"), 
    col("state"),
    col("reading_year"),
    col("reading_month"), 
    col("reading_day"))


var df_reading_ny_a_b = df_reading_ny_a.join(
    df_reading_ny_b, df_reading_ny_a("smart_meter_id")===df_reading_ny_b("smart_meter_id") &&
    df_reading_ny_a("state")===df_reading_ny_b("state") && 
    df_reading_ny_a("reading_year")===df_reading_ny_b("reading_year") &&
    df_reading_ny_a("reading_month")===df_reading_ny_b("reading_month") &&
    df_reading_ny_a("reading_day")===df_reading_ny_b("reading_day") &&
    df_reading_ny_a("reading_hour")===df_reading_ny_b("reading_hour"), "left").select(
        df_reading_ny_a("smart_meter_id"), 
        df_reading_ny_a("reading_hour"), 
        df_reading_ny_a("reading_average_a"), 
        df_reading_ny_a("reading_maximum_a"), 
        df_reading_ny_a("reading_minimum_a"), 
        df_reading_ny_a("average_percent_a"), 
        df_reading_ny_a("maximum_percent_a"), 
        df_reading_ny_a("minimum_percent_a"), 
        df_reading_ny_b("reading_average_b"), 
        df_reading_ny_b("reading_maximum_b"), 
        df_reading_ny_b("reading_minimum_b"), 
        df_reading_ny_b("average_percent_b"), 
        df_reading_ny_b("maximum_percent_b"), 
        df_reading_ny_b("minimum_percent_b"), 
        df_reading_ny_a("state"),
        df_reading_ny_a("reading_year"),
        df_reading_ny_a("reading_month"), 
        df_reading_ny_a("reading_day"))


var df_reading_ny_a_b_c = df_reading_ny_a_b.join(
    df_reading_ny_c, df_reading_ny_a_b("smart_meter_id")===df_reading_ny_c("smart_meter_id") &&
    df_reading_ny_a_b("state")===df_reading_ny_c("state") && 
    df_reading_ny_a_b("reading_year")===df_reading_ny_c("reading_year") &&
    df_reading_ny_a_b("reading_month")===df_reading_ny_c("reading_month") &&
    df_reading_ny_a_b("reading_day")===df_reading_ny_c("reading_day") &&
    df_reading_ny_a_b("reading_hour")===df_reading_ny_c("reading_hour"), "left").select(
        df_reading_ny_a_b("smart_meter_id"), 
        df_reading_ny_a_b("reading_hour"), 
        df_reading_ny_a_b("reading_average_a"), 
        df_reading_ny_a_b("reading_maximum_a"), 
        df_reading_ny_a_b("reading_minimum_a"), 
        df_reading_ny_a_b("average_percent_a"), 
        df_reading_ny_a_b("maximum_percent_a"), 
        df_reading_ny_a_b("minimum_percent_a"), 
        df_reading_ny_a_b("reading_average_b"), 
        df_reading_ny_a_b("reading_maximum_b"), 
        df_reading_ny_a_b("reading_minimum_b"), 
        df_reading_ny_a_b("average_percent_b"), 
        df_reading_ny_a_b("maximum_percent_b"), 
        df_reading_ny_a_b("minimum_percent_b"), 
        df_reading_ny_c("reading_average_c"), 
        df_reading_ny_c("reading_maximum_c"), 
        df_reading_ny_c("reading_minimum_c"), 
        df_reading_ny_c("average_percent_c"), 
        df_reading_ny_c("maximum_percent_c"), 
        df_reading_ny_c("minimum_percent_c"),
        df_reading_ny_a_b("state"),
        df_reading_ny_a_b("reading_year"),
        df_reading_ny_a_b("reading_month"), 
        df_reading_ny_a_b("reading_day"))

<!-- scala> df_reading_ny_a_b_c.printSchema
root
 |-- smart_meter_id: string (nullable = true)
 |-- reading_hour: integer (nullable = true)
 |-- reading_average_a: double (nullable = true)
 |-- reading_maximum_a: double (nullable = true)
 |-- reading_minimum_a: double (nullable = true)
 |-- average_percent_a: double (nullable = true)
 |-- maximum_percent_a: double (nullable = true)
 |-- minimum_percent_a: double (nullable = true)
 |-- reading_average_b: double (nullable = true)
 |-- reading_maximum_b: double (nullable = true)
 |-- reading_minimum_b: double (nullable = true)
 |-- average_percent_b: double (nullable = true)
 |-- maximum_percent_b: double (nullable = true)
 |-- minimum_percent_b: double (nullable = true)
 |-- reading_average_c: double (nullable = true)
 |-- reading_maximum_c: double (nullable = true)
 |-- reading_minimum_c: double (nullable = true)
 |-- average_percent_c: double (nullable = true)
 |-- maximum_percent_c: double (nullable = true)
 |-- minimum_percent_c: double (nullable = true)
 |-- state: string (nullable = true)
 |-- reading_year: string (nullable = true)
 |-- reading_month: string (nullable = true)
 |-- reading_day: string (nullable = true) -->
