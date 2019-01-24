
- Step 0: Create particioning using HIVE SQL
`ALTER TABLE <table> ADD PARTITION(<field>=<field_value>)`

- Step 1: Do the import
`import org.apache.spark.sql.SparkSession`

- Step 2: Start the spark-session
`val spark = SparkSession.builder().appName("StatsAnalyzer").enableHiveSupport().config("hive.exec.dynamic.partition", "true").config("hive.exec.dynamic.partition.mode", "nonstrict").getOrCreate()`

- Step 3: Count the number of rows 
`var df = spark.sql("select * from avangrid_dev.ct_notification")`

- Step 4: See the data 
`df.show`

- Step 5: Print the Schema 
`df.printSchema`

- Step 6: Select the appropirate columns 
`var df = spark.sql("select device_id, notification_time, notification_code, notification_desc, notification_long_desc, notification_type from avangrid_dev.ct_notification")`

- Step 7: Filter based on the date 
`df = df.filter("DT='2018-06-01'")`

- Step 8: Print the Schema 
`df.printSchema`

- Step 9: Select the columns 
- Be make sure the order is as same in 'Schema_SQL_new'

`var state = 'CT'`
...
` df = df.select(col("notification_time"), col("notification_code"), col("notification_desc"), col("notification_long_desc"), col("device_id"), lit(state).as("state"), lit("smart_meter").as("device_type"), col("notification_type"), to_date(col("notification_time")).as("DT"))`

- Step 10: Print the df to see how it looks 
`df.show()`

- Step 11: Do another print schema 
`df.printSchema`

- Step 12: Append these changes to the data frame 
`df.write.mode("Append").insertInto("avangrid_dev.notification")`


Select * from Avangrid_dev.CT_pm B