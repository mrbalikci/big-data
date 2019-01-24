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

var df_transformer = get_transformer(spark)

<!-- scala> df_transformer.printSchema
root
 |-- transformer_id: string (nullable = true)
 |-- latitude: double (nullable = true)
 |-- longitude: double (nullable = true)
 |-- transformer_phase: string (nullable = true)
 |-- transformer_size: string (nullable = true)
 |-- transformer_configuration: string (nullable = true)
 |-- transformer_low_end_configuration: string (nullable = true)
 |-- transformer_low_end_voltage: string (nullable = true)
 |-- transformer_high_end_configuration: string (nullable = true)
 |-- transformer_high_end_voltage: string (nullable = true)
 |-- transformer_installation_date: timestamp (nullable = true)
 |-- transformer_manufacturer: string (nullable = true)
 |-- transformer_bank_id: string (nullable = true)
 |-- transformer_mesh_id: string (nullable = true)
 |-- transformer_feeder_id: string (nullable = true)
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

// JOIN SUBSTATION AND TRANSFORMER ON SUBSTATION ID 
 var df_sub_tran = df_substation.join(df_transformer,
        df_transformer("state")===df_substation("state") &&
        df_transformer("substation_id")===df_substation("substation_id") &&
        df_substation("snapshot_year") === df_feeder("snapshot_year") &&
        df_substation("snapshot_month") === df_feeder("snapshot_month") &&
        df_substation("snapshot_day") === df_feeder("snapshot_day"),"inner"       
        ).select(
            df_substation("state"),
            df_substation("substation_id"),
            df_transformer("transformer_id"),
            df_substation("snapshot_year"),
            df_substation("snapshot_month"),
            df_substation("snapshot_day")
        )

var df_sub_tran_daily = df_sub_tran.join(df_transformer_daily,
        df_transformer_daily("state")===df_sub_tran("state") &&
        df_transformer_daily("transformer_id")===df_sub_tran("transformer_id") &&
        df_transformer_daily("indicators_year") === df_sub_tran("snapshot_year") &&
        df_transformer_daily("indicators_month") === df_sub_tran("snapshot_month") &&
        df_transformer_daily("indicators_day") === df_sub_tran("snapshot_day"),"inner").select(
            df_transformer_daily("state"),
            df_sub_tran("substation_id"),
            df_transformer_daily("transformer_id"),
            df_transformer_daily("indicators_year"),
            df_transformer_daily("indicators_year"),
            df_transformer_daily("indicators_year"),
            df_transformer_daily("transformer_percentage_of_affected_customers"))


---------------------

// JOIN FEEDER AND SUBSTATION ON SUBSTATION ID 

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
        df_substation("snapshot_day"))


// JOIN TRANSFORMER AND FEED_SUBS ON FEEDER ID 
var df_transformer = get_transformer(spark)

<!-- scala> df_transformer.printSchema
root
 |-- transformer_id: string (nullable = true)
 |-- latitude: double (nullable = true)
 |-- longitude: double (nullable = true)
 |-- transformer_phase: string (nullable = true)
 |-- transformer_size: string (nullable = true)
 |-- transformer_configuration: string (nullable = true)
 |-- transformer_low_end_configuration: string (nullable = true)
 |-- transformer_low_end_voltage: string (nullable = true)
 |-- transformer_high_end_configuration: string (nullable = true)
 |-- transformer_high_end_voltage: string (nullable = true)
 |-- transformer_installation_date: timestamp (nullable = true)
 |-- transformer_manufacturer: string (nullable = true)
 |-- transformer_bank_id: string (nullable = true)
 |-- transformer_mesh_id: string (nullable = true)
 |-- transformer_feeder_id: string (nullable = true)
 |-- state: string (nullable = true)
 |-- snapshot_year: string (nullable = true)
 |-- snapshot_month: string (nullable = true)
 |-- snapshot_day: string (nullable = true) -->

 var df_feed_sub_trans = df_feed_subs.join(df_transformer, 
    df_transformer("transformer_feeder_id")===df_feed_subs("feeder_id") &&
    df_transformer("state") === df_feed_subs("state") &&
    df_transformer("snapshot_year") === df_feed_subs("snapshot_year") &&
    df_transformer("snapshot_month") === df_feed_subs("snapshot_month") &&
    df_transformer("snapshot_day") === df_feed_subs("snapshot_day"), "inner").select(
    df_feed_subs("feeder_id"),
    df_feed_subs("substation_id"), 
    df_transformer("transformer_id"),
    df_feed_subs("state"),
    df_feed_subs("snapshot_year"),
    df_feed_subs("snapshot_month"),
    df_feed_subs("snapshot_day"))


<!-- scala> df_feed_sub_trans.printSchema
root
 |-- feeder_id: string (nullable = true)
 |-- substation_id: string (nullable = true)
 |-- transformer_id: string (nullable = true)
 |-- state: string (nullable = true)
 |-- snapshot_year: string (nullable = true)
 |-- snapshot_month: string (nullable = true)
 |-- snapshot_day: string (nullable = true) -->

// GET TRANSFORMER DAILY INDICATORS TO JOIN ON TRANSFORMER ID WITH FEED_SUBS_TRANS

 var df_transformer_daily = get_transformer_daily_indicators(spark)

<!-- scala> df_transformer_daily.printSchema
root
 |-- transformer_id: string (nullable = true)
 |-- transformer_percentage_of_days_out_of_voltage_high: double (nullable = true)
 |-- transformer_percentage_of_days_out_of_voltage_low: double (nullable = true)
 |-- transformer_percentage_of_affected_customers: double (nullable = true)
 |-- transformer_percentage_of_affected_customers_severity: string (nullable = true)
 |-- transformer_customers: double (nullable = true)
 |-- transformer_high_events: double (nullable = true)
 |-- transformer_low_events: double (nullable = true)
 |-- transformer_affected_customers: double (nullable = true)
 |-- state: string (nullable = true)
 |-- indicators_year: string (nullable = true)
 |-- indicators_month: string (nullable = true)
 |-- indicators_day: string (nullable = true) -->

 var df_feed_sub_trans_daily = df_feed_sub_trans.join(df_transformer_daily,
    df_transformer_daily("transformer_id")===df_feed_sub_trans("transformer_id") &&
    df_transformer_daily("state") === df_feed_sub_trans("state") &&
    df_transformer_daily("indicators_year") === df_feed_sub_trans("snapshot_year") &&
    df_transformer_daily("indicators_month") === df_feed_sub_trans("snapshot_month") &&
    df_transformer_daily("indicators_day") === df_feed_sub_trans("snapshot_day"), "inner").select(
        df_feed_sub_trans("feeder_id"),
        df_feed_sub_trans("substation_id"),
        df_feed_sub_trans("transformer_id"),
        df_feed_sub_trans("state"),
        df_transformer_daily("transformer_percentage_of_affected_customers"),
        df_feed_sub_trans("snapshot_year"),
        df_feed_sub_trans("snapshot_month"),
        df_feed_sub_trans("snapshot_day"))


<!-- scala> df_feed_sub_trans_daily.printSchema
root
 |-- feeder_id: string (nullable = true)
 |-- substation_id: string (nullable = true)
 |-- transformer_id: string (nullable = true)
 |-- state: string (nullable = true)
 |-- transformer_percentage_of_affected_customers: double (nullable = true)
 |-- snapshot_year: string (nullable = true)
 |-- snapshot_month: string (nullable = true)
 |-- snapshot_day: string (nullable = true) -->


// JOIN DF_FEED_SUBS_TRANS_DAILY WITH FEEDER DAILY INDICATORS 

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

var df_feed_subs_daily = df_feed_sub_trans_daily.join(df_feeder_ind,
    df_feeder_ind("feeder_id") === df_feed_sub_trans_daily("feeder_id") &&
    df_feeder_ind("state") === df_feed_sub_trans_daily("state") &&
    df_feeder_ind("indicators_year") === df_feed_sub_trans_daily("snapshot_year") &&
    df_feeder_ind("indicators_month") === df_feed_sub_trans_daily("snapshot_month") &&
    df_feeder_ind("indicators_day") === df_feed_sub_trans_daily("snapshot_day"), "left").select(
        df_feed_sub_trans_daily("feeder_id"),
        df_feed_sub_trans_daily("substation_id"),
        df_feed_sub_trans_daily("state"),
        df_feeder_ind("feeder_high_events"),
        df_feeder_ind("feeder_low_events"),
        df_feeder_ind("feeder_customers"),
        df_feeder_ind("feeder_transformers"),
        df_feeder_ind("feeder_affected_customers"),
        df_feeder_ind("feeder_affected_transformers"),
        df_feed_sub_trans_daily("transformer_percentage_of_affected_customers"),
        when(df_feeder_ind("feeder_affected_customers") > 0, 1).otherwise(0).as("substation_affected_feeders"),
        df_feed_sub_trans_daily("snapshot_year"),
        df_feed_sub_trans_daily("snapshot_month"),
        df_feed_sub_trans_daily("snapshot_day"))

    
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
 |-- transformer_percentage_of_affected_customers: double (nullable = true)
 |-- substation_affected_feeders: integer (nullable = false)
 |-- snapshot_year: string (nullable = true)
 |-- snapshot_month: string (nullable = true)
 |-- snapshot_day: string (nullable = true) -->

// GROUP BY FEED_SUBS_DAILY AND AGG 

 var df_feed_daily_indicator = df_feed_subs_daily.groupBy(
    "substation_id", 
    "state",
    "snapshot_year",
    "snapshot_month",
    "snapshot_day").agg(
    sum(col("feeder_high_events")).as("substation_high_events"),
    sum(col("feeder_low_events")).as("substation_low_events"),
    sum(col("feeder_customers")).as("substation_customers"),
    sum(col("feeder_transformers")).as("substation_transformers"),
    countDistinct(col("feeder_id")).as("substation_feeders"),
    sum(col("substation_affected_feeders")).as("substation_affected_feeders"),
    sum(col("feeder_affected_customers")).as("substation_affected_customers"),
    sum(col("feeder_affected_transformers")).as("substatoin_affected_transformers"))


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
 |-- substation_affected_feeders: long (nullable = true)
 |-- substation_affected_customers: double (nullable = true)
 |-- substatoin_affected_transformers: double (nullable = true) -->

// ADD TRANSFORMER PERCENTAGE OF AFFECTED CUSTOMERS TO THE DF FROM DF_FEED_SUBS_DAILY

var df_feed_daily_indicator_final = df_feed_daily_indicator.join(df_feed_subs_daily,
df_feed_subs_daily("substation_id")===df_feed_daily_indicator("substation_id") &&
df_feed_subs_daily("state")===df_feed_daily_indicator("state") &&
df_feed_subs_daily("snapshot_year")===df_feed_daily_indicator("snapshot_year") &&
df_feed_subs_daily("snapshot_month")===df_feed_daily_indicator("snapshot_month") &&
df_feed_subs_daily("snapshot_day")===df_feed_daily_indicator("snapshot_day"), "inner"
).select(
    df_feed_daily_indicator("substation_id"),
    df_feed_daily_indicator("state"),
    df_feed_daily_indicator("snapshot_year"),
    df_feed_daily_indicator("snapshot_month"),
    df_feed_daily_indicator("snapshot_day"),
    df_feed_daily_indicator("substation_high_events"),
    df_feed_daily_indicator("substation_low_events"),
    df_feed_daily_indicator("substation_customers"),
    df_feed_daily_indicator("substation_transformers"),
    df_feed_daily_indicator("substatoin_affected_transformers"),
    df_feed_daily_indicator("substation_feeders"),
    df_feed_daily_indicator("substation_affected_feeders"),
    df_feed_daily_indicator("substation_affected_customers"),
    df_feed_subs_daily("transformer_percentage_of_affected_customers")
)


<!-- scala> df_feed_daily_indicator_final.printSchema
root
 |-- substation_id: string (nullable = true)
 |-- state: string (nullable = true)
 |-- snapshot_year: string (nullable = true)
 |-- snapshot_month: string (nullable = true)
 |-- snapshot_day: string (nullable = true)
 |-- substation_high_events: double (nullable = true)
 |-- substation_low_events: double (nullable = true)
 |-- substation_customers: double (nullable = true)
 |-- substation_transformers: double (nullable = true)
 |-- substatoin_affected_transformers: double (nullable = true)
 |-- substation_feeders: long (nullable = false)
 |-- substation_affected_feeders: long (nullable = true)
 |-- substation_affected_customers: double (nullable = true)
 |-- transformer_percentage_of_affected_customers: double (nullable = true) -->

// CALCULATE SUBSTATION_PERCENTAGE OF AFFECTED CUSTOMER 

var df_feed_daily_indicator_final2 = df_feed_daily_indicator_final.groupBy(
    "substation_id", 
    "state",
    "snapshot_year",
    "snapshot_month",
    "snapshot_day").agg(
    (sum(df_feed_daily_indicator_final("transformer_percentage_of_affected_customers")*df_feed_daily_indicator_final("substation_customers"))/sum(df_feed_daily_indicator_final("substation_customers"))).as("substation_percentage_of_affected_customer"))

<!-- scala> df_feed_daily_indicator_final2.printSchema
root
 |-- substation_id: string (nullable = true)
 |-- state: string (nullable = true)
 |-- snapshot_year: string (nullable = true)
 |-- snapshot_month: string (nullable = true)
 |-- snapshot_day: string (nullable = true)
 |-- substation_percentage_of_affected_customer: double (nullable = true) -->

<!-- 
var df_severity = get_indicator_severity_levels(spark) -->


// GET THE PERSISTENCE SEVERITY TO ADD DF_FEED_DAILY_INDICATOR_FINAL2

var df_severity = spark.sql("select * from avangrid_dev2.indicator_severity_levels where indicator_name='SUBSTATION_AFFECTED_CUSTOMER_PERC'")

// APPLY INDICATOR SEVERITY 
df_feed_daily_indicator_final2 = df_feed_daily_indicator_final2.join(df_severity, 
    df_feed_daily_indicator_final2("substation_percentage_of_affected_customer") > df_severity("indicator_lower_bound") && 
    df_feed_daily_indicator_final2("substation_percentage_of_affected_customer") <= df_severity("indicator_higher_bound"), "inner").select(
        df_feed_daily_indicator_final2("substation_id"),
        df_feed_daily_indicator_final2("state"),
        df_feed_daily_indicator_final2("snapshot_year"),
        df_feed_daily_indicator_final2("snapshot_month"),
        df_feed_daily_indicator_final2("snapshot_day"),
        df_severity("indicator_severity"),
        df_feed_daily_indicator_final2("substation_percentage_of_affected_customer"))

<!-- scala> df_feed_daily_indicator_final2.printSchema
root
 |-- substation_id: string (nullable = true)
 |-- state: string (nullable = true)
 |-- snapshot_year: string (nullable = true)
 |-- snapshot_month: string (nullable = true)
 |-- snapshot_day: string (nullable = true)
 |-- indicator_severity: string (nullable = true) -->


<!-- scala> df_feed_daily_indicator_final.printSchema
root
 |-- substation_id: string (nullable = true)
 |-- state: string (nullable = true)
 |-- snapshot_year: string (nullable = true)
 |-- snapshot_month: string (nullable = true)
 |-- snapshot_day: string (nullable = true)
 |-- substation_high_events: double (nullable = true)
 |-- substation_low_events: double (nullable = true)
 |-- substation_customers: double (nullable = true)
 |-- substation_transformers: double (nullable = true)
 |-- substatoin_affected_transformers: double (nullable = true)
 |-- substation_feeders: long (nullable = false)
 |-- substation_affected_feeders: long (nullable = true)
 |-- substation_affected_customers: double (nullable = true)
 |-- transformer_percentage_of_affected_customers: double (nullable = true) -->


// JOIN THE TWO FINAL AND FINAL2 DFs

df_feed_daily_indicator_final.join(df_feed_daily_indicator_final2, 
    df_feed_daily_indicator_final2("substation_id")===df_feed_daily_indicator_final("substation_id") &&
    df_feed_daily_indicator_final2("state")===df_feed_daily_indicator_final("state") &&
    df_feed_daily_indicator_final2("snapshot_year")===df_feed_daily_indicator_final("snapshot_year") &&
    df_feed_daily_indicator_final2("snapshot_month")===df_feed_daily_indicator_final("snapshot_month") &&
    df_feed_daily_indicator_final2("snapshot_day")===df_feed_daily_indicator_final("snapshot_day")
).select(
    df_feed_daily_indicator_final("substation_id"),
    df_feed_daily_indicator_final("state"),
    df_feed_daily_indicator_final("snapshot_year"),
    df_feed_daily_indicator_final("snapshot_month"),
    df_feed_daily_indicator_final("snapshot_day"),
    df_feed_daily_indicator_final2("substation_percent_of_affected_customers").as("substation_percentage_affected_customers"),
    df_feed_daily_indicator_final2("indicator_severity").as("substation_percentage_affected_customers_severity"),
    df_feed_daily_indicator_final("substation_high_events"),
    df_feed_daily_indicator_final("substation_low_events"),
    df_feed_daily_indicator_final("substation_customers"),
    df_feed_daily_indicator_final("substation_transformers"),
    df_feed_daily_indicator_final("substatoin_affected_transformers"),
    df_feed_daily_indicator_final("substation_feeders"),
    df_feed_daily_indicator_final("substation_affected_feeders"),
    df_feed_daily_indicator_final("substation_affected_customers"))