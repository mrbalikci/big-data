
- Import dependencies 

`import org.apache.spark.sql.SparkSession`
`import org.apache.spark.sql.types._`
`import org.apache.spark.sql.functions._`

- Start the park session, dynamic partition: true

`val spark = SparkSession.builder().appName("StatsAnalyzer").enableHiveSupport().config("hive.exec.dynamic.partition", "true").config("hive.exec.dynamic.partition.mode", "nonstrict").getOrCreate()`

- Define the reading format based on the csv file delimiter 

1. HiveContext 
` var dr = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", "~").option("comment","H")` 

2. Non-Hive Context 

`var dr = spark.read.format("com.databricks.spark.csv").option("delimiter", "~").option("comment","H")`

- Read the csv file in tmp 

`var df = dr.load("/tmp/...")`

- See the df the DataFrame

`df.show()`

- Do some cleaning if it is necessary 

- Filter for T, if you'd like to T
`df.filter("_c0='T'").show()`

- Filter for D and save as new df 
`df.filter("_c0='D'").show()`

- save a new df
`df = df.filter("_c0='D'")`

- count number of rows 
`df.count()`
1. One way 

`df = df.select("_c2", "_c7", "_c5", "_c6", "_c9", "_c8").toDF("meter_id", "time", "notification_code", "notification_desc", "notification_long_desc", "notification_type")`

2. Another way: a Better Way

`df = df.select(col("_c2").as("meter_id"),
col("_c7").as("time"),
col("_c5").as("notification_code"),
col("_c6").as("notification_desc"),
col("_c9").as("notification_long_desc"),
col("_c8").as("notification_type"),
to_date(col("_c7")).as("DT"),
lit("smart_meter").as("device_type"),
lit("CT").as("state"))`

- Schema: 
 |-- row_type: string (nullable = true)
 |-- device_type: string (nullable = true)
 |-- device_id: string (nullable = true)
 |-- acct: string (nullable = true)
 |-- acct2: string (nullable = true)
 |-- notification_code: string (nullable = true)
 |-- notification_desc: string (nullable = true)
 |-- notification_time: string (nullable = true)
 |-- notification_type: string (nullable = true)
 |-- notification_long_desc: string (nullable = true)
 |-- notification_time2: string (nullable = true)
 |-- dt: string (nullable = true)

- The select code / scala 
var df = spark.sql("select device_id, notification_time, notification_code, notification_desc, notification_long_desc, notification_type from avangrid_dev.ct_notification")

// this is the argument, we need to tell this 
// to process the files for this particular date for 
// particianing
df = df.filter(DT="2018-06-01")

- Select appropirate columns 
df = df.select(col("device_id"), col("notification_time"), col("notification_code"), col("notification_desc"), col("notification_long_desc"), col("notification_type"), to_date(col("notification_time")).as("DT"), lit("smart_meter").as("device_type"), lit("CT").as("state"))

- append it to the db table in Ambari 
df.write.mode("Append").insertInto("avangrid_dev.notification")
- See the df 

`df.show()`

- Be make sure the order of columns is the same as the `notification` table in Hive

 `df.select("meter_id","notification_code", "notification_desc", "notification_long_desc","time", "state","notification_type","device_type","DT").show()`

 - Sassing newly selected columns to the df 

 `df=df.select("meter_id","notification_code", "notification_desc", "notification_long_desc","time", "state","notification_type","device_type","DT")`

- Append the new df to the notification in the database that was created in Sandbox
- It will take time depending on the size of the file 

`df.write.mode("Append").insertInto("something_dev.notification")`

- After appending the newly df to db, go back to Ambari > Hive View > Select db on the left > Select the table > Load the sample data:

`SELECT * FROM notification LIMIT 100;`

- Find out the count of rows 

` SELECT COUNT(*) FROM notification LIMIT 100;`