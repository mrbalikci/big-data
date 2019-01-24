
1. Import dependencies 
`import org.apache.spark.sql.SparkSession`

2. Start spark session 
`val spark = SparkSession.builder().appName("StatsAnalyzer").enableHiveSupport().config("hive.exec.dynamic.partition", "true").config("hive.exec.dynamic.partition.mode", "nonstrict").getOrCreate()`

3. Define the data frame and read it the rows from the db
`var df = spark.sql("select * from avangrid_dev.ct_notification")`

4. Filter by the date 
`df = df.filter("DT='2018-06-01'")`

5. Filter the state 
`var state = "CT"`

6. Select the approprate columns 
`df = df.select(col("notification_time"), col("notification_code"), col("notification_desc"), col("notification_long_desc"), col("device_id"), lit(state).as("state"), lit("smart_meter").as("device_type"), col("notification_type"), to_date(col("notification_time")).as("DT"))`

7. Append the data to the table
`df.write.mode("Append").insertInto("avangrid_dev.notification")`