# How to read XML file in spark 

- Dependencies 
`import org.apache.spark.sql.SparkSession`
`import org.apache.spark.sql.types._`
`val spark = SparkSession.builder().appName("StatsAnalyzer").enableHiveSupport().config("hive.exec.dynamic.partition", "true").config("hive.exec.dynamic.partition.mode", "nonstrict").getOrCreate()`
`val dr = spark.read.format("com.databricks.spark.xml")`

- read the file in tmp 
- be make sure the file is in the hdfs 

`var df = dr.load("/tmp/something_test_load/557202_20180721195927481.xml")`

- the df will be empty because of XML structure 

`df.show` 
`df.printSchema` will give you the type of schema which is empty `root`

- It is difficult to read `XML` files unless you know what you are doing 
- internet explorer can be used to read the file 

- We need to add the `rootTag` and `rowTag` from the `XML` structure 

val dr = spark.read.format("com.databricks.spark.xml").option("rootTag", "Channels").option("rowTag", "Channel")

- Reload the file, it will take time to reload

`var df = dr.load("/tmp/something_test_load/557202_20180721195927481.xml")`

- do `df.show` 

- the issue is there is no clear reading 
- to solve it

`df.printSchema` will give you the structure of the XML file 
- it will give you the root, row and type structure of the data: The Road Map

root
 |-- ChannelID: struct (nullable = true)
 |    |-- _ServicePointChannelID: string (nullable = true)
 |    |-- _VALUE: string (nullable = true)
 |-- ContiguousIntervalSets: struct (nullable = true)
 |    |-- ContiguousIntervalSet: struct (nullable = true)
 |    |    |-- Readings: struct (nullable = true)
 |    |    |    |-- Reading: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- ReadingStatus: struct (nullable = true)
 |    |    |    |    |    |    |-- UnencodedStatus: struct (nullable = true)
 |    |    |    |    |    |    |    |-- _SourceValidation: string (nullable = true)
 |    |    |    |    |    |    |    |-- _VALUE: string (nullable = true)
 |    |    |    |    |    |-- _Value: long (nullable = true)
 |    |    |-- TimePeriod: struct (nullable = true)
 |    |    |    |-- _EndTime: string (nullable = true)
 |    |    |    |-- _StartTime: string (nullable = true)
 |    |    |    |-- _VALUE: string (nullable = true)
 |    |    |-- _NumberOfReadings: long (nullable = true)
 |-- _IntervalLength: long (nullable = true)
 |-- _IsReadingDecoded: boolean (nullable = true)
 |-- _IsRegister: boolean (nullable = true)
 |-- _MarketType: string (nullable = true)
 |-- _NumberOfDials: long (nullable = true)
 |-- _PressureCompensationFactor: long (nullable = true)
 |-- _PulseMultiplier: long (nullable = true)
 |-- _ReadingsInPulse: boolean (nullable = true)

- To select a column from this `XML` `root`

`df.select(col("ChannelID._ServicePointChannelID").as("meter_id")).show` 

- Read the `XML` root structure and convert it to df 

`df.select(regexp_replace(col("ChannelID._ServicePointChannelID").cast(StringType),":930","").as("meter_id"),col("_intervalLength").as("IntervalLength"),unix_timestamp(col("ContiguousIntervalSets.ContiguousIntervalSet.TimePeriod._StartTime"),"yyyy-MM-dd'T'HH:mm:ss'Z'").as("StartTime"),unix_timestamp(col("ContiguousIntervalSets.ContiguousIntervalSet.TimePeriod._EndTime"),"yyyy-MM-dd'T'HH:mm:ss'Z'").as("EndTime"),posexplode(col("ContiguousIntervalSets.ContiguousIntervalSet.Readings.Reading._Value"))).show`

- Assing the above code to a new `Val`

`val exploded = df.select(regexp_replace(col("ChannelID._ServicePointChannelID").cast(StringType),":930","").as("meter_id"),col("_intervalLength").as("IntervalLength"),unix_timestamp(col("ContiguousIntervalSets.ContiguousIntervalSet.TimePeriod._StartTime"),"yyyy-MM-dd'T'HH:mm:ss'Z'").as("StartTime"),unix_timestamp(col("ContiguousIntervalSets.ContiguousIntervalSet.TimePeriod._EndTime"),"yyyy-MM-dd'T'HH:mm:ss'Z'").as("EndTime"),posexplode(col("ContiguousIntervalSets.ContiguousIntervalSet.Readings.Reading._Value")))`

- Format the new Val `exploded` 

`val formated = exploded.select(col("meter_id"), (col("StartTime")+col("pos")*col("IntervalLength")*60).cast(TimestampType).as("time"),to_date((col("StartTime")+col("pos")*col("IntervalLength")*60).cast(TimestampType)).as("dt"), col("col").as("reading")).withColumn("reading_type", lit("VAC"))`

- See `formated.show` the see the df

