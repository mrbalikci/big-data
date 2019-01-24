Timestamp,                 Value
2014-01-01 00:00:01,       3.0
2014-01-01 00:00:05,       12.0
2014-01-01 00:00:09,       8.0
2014-01-01 00:00:10,       45.0
2014-01-01 00:00:15,       3.0
2014-01-01 00:00:21,       4.0
2014-01-01 00:00:32,       19.0

val df1 = Seq(
    ("2014-01-01 00:00:01", 3.0),
    ("2014-01-01 00:00:05", 12.0),
    ("2014-01-01 00:00:09", 8.0),
    ("2014-01-01 00:00:10", 45.0),
    ("2014-01-01 00:00:15", 3.0),
    ("2014-01-01 00:00:21", 4.0),
    ("2014-01-01 00:00:32", 19.0)).toDF("Timestamp", "Value")


Timestamp,                 Value
2014-01-01 00:00:01,       3.0
2014-01-01 00:00:10,       45.0
,       8.0

val df2 = Seq(
    ("2014-01-01 00:00:01", 3.0),
    ("2014-01-01 00:00:10", 45.0),
    ("2014-01-01 00:00:09", 8.0)).toDF("Timestamp", "Value")

// we need to sort values to enable fast searching using binary search
val values = df2.collect().map(r => r.getDouble(0)).sorted
val valuesBroadcast = session.sparkContext.broadcast(values)


https://stackoverflow.com/questions/44782586/how-to-find-the-nearest-value-of-two-dataframes-in-spark

