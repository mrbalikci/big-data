
1. import org.apache.spark.sql.SparkSession
2. val spark = SparkSession.builder().appName("StatsAnalyzer").enableHiveSupport().config("hive.exec.dynamic.partition", "true").config("hive.exec.dynamic.partition.mode", "nonstrict").getOrCreate()
3. var df = spark.sql("select * from avangrid_dev.ny_notification")
4. df = df.filter("DT='2018-06-01'")
5. var state = "NY"
6. df = df.select(col("eventdatetime").as("notification_time"), lit("none").as("notification_code"), col("eventdescription").as("notification_desc"), col("eventlongdescription").as("notification_long_desc"), col("deviceid").as("device_id"), lit("NY").as("state"), lit("smart_meter").as("device_type"), col("eventtype").as("notification_type"), to_date(col("eventdatetime")).as("DT"))
7. df.write.mode("Append").insertInto("avangrid_dev.notification")