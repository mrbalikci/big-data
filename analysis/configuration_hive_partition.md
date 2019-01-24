import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("StatsAnalyzer").enableHiveSupport().config("hive.exec.dynamic.partition", "true").config("hive.exec.dynamic.partition.mode", "nonstrict").config("hive.exec.max.dynamic.partitions.pernode","10000").config("hive.exec.max.dynamic.partitions","100000").config("hive.optimize.sort.dynamic.partition","true").getOrCreate()

