// JOIN FEEDER AND SUBSTATION 

var df_feeder = get_feeder(spark)

<!-- scala> get_feeder(spark).printSchema
root
 |-- feeder_id: string (nullable = true)
 |-- substation_bus_id: string (nullable = true)
 |-- substation_id: string (nullable = true)
 |-- state: string (nullable = true)
 |-- snapshot_year: string (nullable = true)
 |-- snapshot_month: string (nullable = true)
 |-- snapshot_day: string (nullable = true) -->

var df_substation = get_substation(spark)

<!-- scala> get_substation(spark).printSchema
root
 |-- substation_id: string (nullable = true)
 |-- substation_name: string (nullable = true)
 |-- state: string (nullable = true)
 |-- snapshot_year: string (nullable = true)
 |-- snapshot_month: string (nullable = true)
 |-- snapshot_day: string (nullable = true) -->

var df_feed_subs = df_feeder.join(df_substation, 
    df_substation("substation_id")===df_feeder("substation_id") &&
    df_substation("state") === df_feeder("state") &&
    df_substation("snapshot_year") === df_feeder("snapshot_year") &&
    df_substation("snapshot_month") === df_feeder("snapshot_month") &&
    df_substation("snapshot_day") === df_feeder("snapshot_day"), "left").select(
        df_feeder("feeder_id"),
        df_substation("substation_id"), 
        df_substation("state"),
        df_substation("snapshot_year"),
        df_substation("snapshot_month"),
        df_substation("snapshot_day")
    )

<!-- scala> df_feed_subs.printSchema
root
 |-- feeder_id: string (nullable = true)
 |-- substation_id: string (nullable = true)
 |-- state: string (nullable = true)
 |-- snapshot_year: string (nullable = true)
 |-- snapshot_month: string (nullable = true)
 |-- snapshot_day: string (nullable = true) -->

// JOIN DF_FEED_SUBS WITH FEEDER DAILY INDICATORS 

var df_feeder_ind = get_feeder_daily_indicators(spark)

<!-- scala> df_feeder_ind.printSchema
root
 |-- feeder_id: string (nullable = true)
 |-- feeder_percentage_of_affected_customers: double (nullable = true)
 |-- feeder_percentage_of_affected_customers_severity: double (nullable = true)
 |-- feeder_high_events: double (nullable = true)
 |-- feeder_low_events: double (nullable = true)
 |-- feeder_customers: double (nullable = true)
 |-- feeder_transformers: double (nullable = true)
 |-- feeder_affected_customers: double (nullable = true)
 |-- feeder_affected_transformers: double (nullable = true)
 |-- state: string (nullable = true)
 |-- indicators_year: string (nullable = true)
 |-- indicators_month: string (nullable = true)
 |-- indicators_day: string (nullable = true) -->

var df_feed_subs_daily = df_feed_subs.join(df_feeder_ind,
    df_feeder_ind("feeder_id") === df_feed_subs("feeder_id") &&
    df_feeder_ind("state") === df_feed_subs("state") &&
    df_feeder_ind("indicators_year") === df_feed_subs("snapshot_year") &&
    df_feeder_ind("indicators_month") === df_feed_subs("snapshot_month") &&
    df_feeder_ind("indicators_day") === df_feed_subs("snapshot_day"), "left").select(
        df_feed_subs("feeder_id"),
        df_feed_subs("substation_id"),
        df_feed_subs("state"),
        df_feeder_ind("feeder_high_events"),
        df_feeder_ind("feeder_low_events"),
        df_feeder_ind("feeder_customers"),
        df_feeder_ind("feeder_transformers"),
        df_feeder_ind("feeder_affected_customers"),
        df_feeder_ind("feeder_affected_transformers"),
        when(df_feeder_ind("feeder_affected_customers") > 0, 1).otherwise(0).as("substation_affected_feeders"),
        df_feed_subs("snapshot_year"),
        df_feed_subs("snapshot_month"),
        df_feed_subs("snapshot_day"))

// FILTER BY STATE 
df_feed_subs_daily =  df_feed_subs_daily.filter("state='CT'")


<!-- scala> df_feed_subs_daily.printSchema
root
 |-- feeder_id: string (nullable = true)
 |-- substation_id: string (nullable = true)
 |-- state: string (nullable = true)
 |-- feeder_high_events: double (nullable = true)
 |-- feeder_low_events: double (nullable = true)
 |-- feeder_customers: double (nullable = true)
 |-- feeder_transformers: double (nullable = true)
 |-- feeder_affected_customers: double (nullable = true)
 |-- feeder_affected_transformers: double (nullable = true)
 |-- substation_affected_feeders: integer (nullable = false)
 |-- snapshot_year: string (nullable = true)
 |-- snapshot_month: string (nullable = true)
 |-- snapshot_day: string (nullable = true) -->


var df_feed_daily_indicator = df_feed_subs_daily.groupBy(
    "substation_id", 
    "state",
    "snapshot_year",
    "snapshot_month",
    "snapshot_day").agg(
    lit("none").as("substation_percent_of_affected_customers"),
    lit("none").as("substation_per_affec_custo_severity"),
    sum(col("feeder_high_events")).as("substation_high_events"),
    sum(col("feeder_low_events")).as("substation_low_events"),
    sum(col("feeder_customers")).as("substation_customers"),
    sum(col("feeder_transformers")).as("substation_transformers"),
    countDistinct(col("feeder_id")).as("substation_feeders"),
    sum(col("substation_affected_feeders")).as("substation_affected_feeders"),
    sum(col("feeder_affected_customers")).as("substation_affected_customers"),
    sum(col("feeder_affected_transformers")).as("substatoin_affected_transformers"))


countDistinct(col("feeder_id")).where(col("feeder_affected_customers") > 0),

<!-- scala> df_feed_daily_indicator.printSchema
root
 |-- substation_id: string (nullable = true)
 |-- state: string (nullable = true)
 |-- snapshot_year: string (nullable = true)
 |-- snapshot_month: string (nullable = true)
 |-- snapshot_day: string (nullable = true)
 |-- substation_percent_of_affected_customers: string (nullable = false)
 |-- substation_per_affec_custo_severity: string (nullable = false)
 |-- substation_high_events: double (nullable = true)
 |-- substation_low_events: double (nullable = true)
 |-- substation_customers: double (nullable = true)
 |-- substation_transformers: double (nullable = true)
 |-- substation_feeders: long (nullable = false)
 |-- substation_affected_customers: double (nullable = true)
 |-- substatoin_affected_transformers: double (nullable = true) -->

 df_feed_daily_indicator.withColumn("substation_affected_feeders",col("feeder_affected_customers") > 0, countDistinct(col("feeder_id"))).otherwise(0)

 when(df_event("event_type") === "Over Voltage", df_voltage("maximum")).otherwise(df_voltage("minimum")).as("voltage")

 when(col("feeder_affected_customers") > 0, countDistinct(col("feeder_id"))).otherwise(0).as("substation_affected_feeders")


df_feed_subs_daily.groupBy(
    "substation_id", 
    "state",
    "snapshot_year",
    "snapshot_month",
    "snapshot_day").agg(
    lit("none").as("substation_percent_of_affected_customers"),
    lit("none").as("substation_per_affec_custo_severity"),
    sum(col("feeder_high_events")).as("substation_high_events"),
    sum(col("feeder_low_events")).as("substation_low_events"),
    sum(col("feeder_customers")).as("substation_customers"),
    sum(col("feeder_transformers")).as("substation_transformers"),
    countDistinct(col("feeder_id")).as("substation_feeders"),
    sum(col("feeder_affected_customers")).as("substation_affected_customers"),
    sum(col("feeder_affected_transformers")).as("substation_affected_transformers"))