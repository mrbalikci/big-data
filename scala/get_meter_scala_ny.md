// FILTER BY THE DATE 
var df_proc_date = spark.sql("select current_date proc_date")

df_proc_date = spark.sql("select to_date('2018-10-01') proc_date")

df_proc_date = df_proc_date.select(
     |       date_format(df_proc_date("proc_date"),"YYYY").as("YEAR"),
     |       date_format(df_proc_date("proc_date"),"MM").as("MONTH"),
     |       date_format(df_proc_date("proc_date"),"dd").as("DAY"))


df_proc_date.show

var df_meter = get_smart_meter(spark)

df_meter = df_meter.join(df_proc_date,
    df_meter("snapshot_year") === df_proc_date("YEAR") &&
    df_meter("snapshot_month") === df_proc_date("MONTH") &&
    df_meter("snapshot_day") === df_proc_date("DAY"),
  "inner").select(
  df_meter("smart_meter_id"),
  df_meter("state"),
  df_meter("reference_voltage_a"),
  df_meter("reference_voltage_b"),
  df_meter("reference_voltage_c"),
  df_meter("snapshot_year"),
  df_meter("snapshot_month"),
  df_meter("snapshot_day"))

df_meter.show




<!-- // PIVOT 
var df_voltage_averages = get_reading(spark).groupBy(col("state"),col("device_id")).pivot("reading_type").agg(mean("reading_value"))

var df_voltage_ny = df_voltage_averages.filter("state='NY'") -->