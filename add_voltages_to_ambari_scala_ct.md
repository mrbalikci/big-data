// import dependencies 
import org.apache.spark.sql.SparkSession

// start the spark sesson
val spark = SparkSession.builder().appName("StatsAnalyzer").enableHiveSupport().config("hive.exec.dynamic.partition", "true").config("hive.exec.dynamic.partition.mode", "nonstrict").getOrCreate()

// read the daily voltages for ct
var df = spark.sql("select * from avangrid_dev.ct_daily_voltage")

// count the df size
df.count()

// print the schema
df.printSchema()

// read max voltages 
var df_max = df.select(col("reading_dt").as("reading_time"), col("maximum_voltage"), lit("v").as("reading_unit"), col("device_id"), lit("CT").as("state"), lit("smart_meter").as("device_type"), lit("voltage_phase_A").as("reading_type"), col("reading_dt"))

// make a filter where reading value is not null
df_max = df_max.filter("maximum_voltage <>''")

// convert the timestamp
val temp_max  = df_max.withColumn("reading_dt", from_unixtime(unix_timestamp(col("reading_dt"), "MM/dd/yy"), "yyyy-MM-dd"))
val temp_max2  = temp_max.withColumn("reading_time", from_unixtime(unix_timestamp(col("reading_time"), "MM/dd/yy"), "yyyy-MM-dd"))

// read the min voltages
var df_min = df.select(col("reading_dt").as("reading_time"), col("minimum_voltage"), lit("v").as("reading_unit"), col("device_id"), lit("CT").as("state"), lit("smart_meter").as("device_type"), lit("voltage_phase_A").as("reading_type"), col("reading_dt"))

// make a filter where reading value is not null 
df_min = df_min.filter("minimum_voltage <>''")

// convert the timestamp
val temp_min  = df_min.withColumn("reading_dt", from_unixtime(unix_timestamp(col("reading_dt"), "MM/dd/yy"), "yyyy-MM-dd"))
val temp_min2  = temp_min.withColumn("reading_time", from_unixtime(unix_timestamp(col("reading_time"), "MM/dd/yy"), "yyyy-MM-dd"))

// read the instant voltages 
var df_inst = df.select(col("reading_dt").as("reading_time"), col("instant_voltage"), lit("v").as("reading_unit"), col("device_id"), lit("CT").as("state"), lit("smart_meter").as("device_type"), lit("voltage_phase_A").as("reading_type"), col("reading_dt"))

// make a filter where reading value is not null 
df_inst = df_inst.filter("instant_voltage <>''")

// convert the timestamp 
val temp_inst  = df_inst.withColumn("reading_dt", from_unixtime(unix_timestamp(col("reading_dt"), "MM/dd/yy"), "yyyy-MM-dd"))
val temp_inst2  = df_inst.withColumn("reading_time", from_unixtime(unix_timestamp(col("reading_time"), "MM/dd/yy"), "yyyy-MM-dd"))

// append all the changes to reading table in ambari hive 
temp_min2.write.mode("Append").insertInto("avangrid_dev.reading")
temp_max2.write.mode("Append").insertInto("avangrid_dev.reading")
temp_inst2.write.mode("Append").insertInto("avangrid_dev.reading")
