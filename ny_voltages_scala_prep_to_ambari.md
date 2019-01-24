1. import org.apache.spark.sql.SparkSession
2. val spark = SparkSession.builder().appName("StatsAnalyzer").enableHiveSupport().config("hive.exec.dynamic.partition", "true").config("hive.exec.dynamic.partition.mode", "nonstrict").getOrCreate()
4. var state = "NY"
5. var df = spark.sql("select * from avangrid_dev.ny_voltage_measures")
6. df = df.select(col("reading_time"), col("instant_voltage").as("reading_value"), lit("v").as("reading_unit"), col("device_id"), lit("NY").as("state"), lit("smart_meter").as("device_type"), lit("Voltage Phase A").as("reading_type"),  to_date(col("reading_time")).as("reading_dt"))
7. df.write.mode("Append").insertInto("avangrid_dev.reading")

