// GET OUT OF VOLTAGE EVENTS 

var df_subs = get_out_of_voltage_event(spark)

<!-- scala> df_subs.printSchema
root
 |-- event_start_time: timestamp (nullable = true)
 |-- event_end_time: timestamp (nullable = true)
 |-- smart_meter_id: string (nullable = true)
 |-- event_elapsed_time: double (nullable = true)
 |-- event_description: string (nullable = true)
 |-- event_voltage_phase: string (nullable = true)
 |-- event_max_voltage: double (nullable = true)
 |-- event_min_voltage: double (nullable = true)
 |-- event_mean_voltage: double (nullable = true)
 |-- event_out_of_voltage_percentage: double (nullable = true)
 |-- voltage_severity: string (nullable = true)
 |-- time_severity: string (nullable = true)
 |-- overall_severity: string (nullable = true)
 |-- state: string (nullable = true)
 |-- event_type: string (nullable = true)
 |-- event_start_year: string (nullable = true)
 |-- event_start_month: string (nullable = true)
 |-- event_start_day: string (nullable = true) -->

// FILTER FOR CT 
df_subs = df_subs.filter("state='CT'")

// GET SMART METER DATA
// FILTER STATE >> PARAMETERIZE IT 
var df_smart_meter = get_smart_meter(spark).filter("state='CT'")


<!-- scala> df_smart_meter.printSchema
root
 |-- smart_meter_id: string (nullable = true)
 |-- latitude: double (nullable = true)
 |-- longitude: double (nullable = true)
 |-- city: string (nullable = true)
 |-- address: string (nullable = true)
 |-- zipcode: string (nullable = true)
 |-- revenue_class: string (nullable = true)
 |-- rate_plan: string (nullable = true)
 |-- der_customer: string (nullable = true)
 |-- transformer_id: string (nullable = true)
 |-- transformer_bank_id: string (nullable = true)
 |-- transformer_mesh_id: string (nullable = true)
 |-- reference_voltage_a: double (nullable = true)
 |-- reference_voltage_b: double (nullable = true)
 |-- reference_voltage_c: double (nullable = true)
 |-- state: string (nullable = true)
 |-- snapshot_year: string (nullable = true)
 |-- snapshot_month: string (nullable = true)
 |-- snapshot_day: string (nullable = true) -->

// Use the table SMART_METER to gather information about the smart meters

var df_sub_meter = df_smart_meter.join(df_subs,
    df_subs("state") === df_smart_meter("state") &&
    df_subs("smart_meter_id") === df_smart_meter("smart_meter_id") &&
    df_subs("event_start_year") === df_smart_meter("snapshot_year") &&
    df_subs("event_start_month") === df_smart_meter("snapshot_month") &&
    df_subs("event_start_day") === df_smart_meter("snapshot_day"), "left"
).select(
    df_smart_meter("state"),
    df_smart_meter("smart_meter_id"),
    df_smart_meter("transformer_id"),
    df_smart_meter("snapshot_year"),
    df_smart_meter("snapshot_month"),
    df_smart_meter("snapshot_day"),
    df_subs("event_type")
)

<!-- scala> df_sub_meter.printSchema
root
 |-- state: string (nullable = true)
 |-- smart_meter_id: string (nullable = true)
 |-- transformer_id: string (nullable = true)
 |-- snapshot_year: string (nullable = true)
 |-- snapshot_month: string (nullable = true)
 |-- snapshot_day: string (nullable = true)
 |-- event_type: string (nullable = true) -->

// Use the table TRANSFORMER to gather information about the transformers. 
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

  var df_smart_tran = df_sub_meter.join(df_transformer,
    df_transformer("state") === df_sub_meter("state") &&
    df_transformer("transformer_id") === df_sub_meter("transformer_id") &&
    df_transformer("snapshot_year") === df_sub_meter("snapshot_year") &&
    df_transformer("snapshot_month") === df_sub_meter("snapshot_month") &&
    df_transformer("snapshot_day") === df_sub_meter("snapshot_day"), "left"
).select(
    df_sub_meter("state"),
    df_sub_meter("smart_meter_id"),
    df_sub_meter("transformer_id"),
    df_sub_meter("event_type"),
    df_sub_meter("snapshot_year"),
    df_sub_meter("snapshot_month"),
    df_sub_meter("snapshot_day"))

<!-- scala> df_smart_tran.printSchema
root
 |-- state: string (nullable = true)
 |-- smart_meter_id: string (nullable = true)
 |-- transformer_id: string (nullable = true)
 |-- event_type: string (nullable = true)
 |-- snapshot_year: string (nullable = true)
 |-- snapshot_month: string (nullable = true)
 |-- snapshot_day: string (nullable = true) -->

//	Use the table FEEDER to gather information about the feeders. 

var df_feeder = get_feeder(spark)

<!-- scala> df_feeder.printSchema
root
 |-- feeder_id: string (nullable = true)
 |-- substation_bus_id: string (nullable = true)
 |-- substation_id: string (nullable = true)
 |-- state: string (nullable = true)
 |-- snapshot_year: string (nullable = true)
 |-- snapshot_month: string (nullable = true)
 |-- snapshot_day: string (nullable = true) -->

var df_tran_feeder = df_transformer.join(df_feeder,
    df_feeder("state") === df_transformer("state") &&
    df_feeder("feeder_id") === df_transformer("transformer_feeder_id") &&
    df_feeder("snapshot_year") === df_transformer("snapshot_year") &&
    df_feeder("snapshot_month") === df_transformer("snapshot_month") &&
    df_feeder("snapshot_day") === df_feeder("snapshot_day"), "left").select(
    df_transformer("state"),
    df_transformer("transformer_id"),
    df_transformer("transformer_feeder_id"),
    df_transformer("snapshot_year"),
    df_transformer("snapshot_month"),
    df_transformer("snapshot_day"))


<!-- scala> df_tran_feeder.printSchema
root
 |-- state: string (nullable = true)
 |-- transformer_id: string (nullable = true)
 |-- transformer_feeder_id: string (nullable = true)
 |-- snapshot_year: string (nullable = true)
 |-- snapshot_month: string (nullable = true)
 |-- snapshot_day: string (nullable = true) -->

<!-- scala> df_smart_tran.printSchema
root
 |-- state: string (nullable = true)
 |-- smart_meter_id: string (nullable = true)
 |-- transformer_id: string (nullable = true)
 |-- event_type: string (nullable = true)
 |-- snapshot_year: string (nullable = true)
 |-- snapshot_month: string (nullable = true)
 |-- snapshot_day: string (nullable = true) -->

 // JOIN DF_TRAN_FEEDER WITH DF_SMART_TRAN 

 var df_smart_feeder = df_smart_tran.join(df_tran_feeder,
    df_tran_feeder("state")===df_smart_tran("state") &&
    df_tran_feeder("transformer_id") === df_smart_tran("transformer_id") &&
    df_tran_feeder("snapshot_year") === df_smart_tran("snapshot_year") &&
    df_tran_feeder("snapshot_month") === df_smart_tran("snapshot_month") &&
    df_tran_feeder("snapshot_day") === df_smart_tran("snapshot_day"), "left").select(
    df_smart_tran("state"),
    df_smart_tran("smart_meter_id"),
    df_smart_tran("transformer_id"),
    df_tran_feeder("transformer_feeder_id").as("feeder_id"),
    df_smart_tran("event_type"),
    df_smart_tran("snapshot_year"),
    df_smart_tran("snapshot_month"),
    df_smart_tran("snapshot_day"))

<!-- scala> df_smart_feeder.printSchema
root
 |-- state: string (nullable = true)
 |-- smart_meter_id: string (nullable = true)
 |-- transformer_id: string (nullable = true)
 |-- feeder_id: string (nullable = true)
 |-- event_type: string (nullable = true)
 |-- snapshot_year: string (nullable = true)
 |-- snapshot_month: string (nullable = true)
 |-- snapshot_day: string (nullable = true) -->

//	Use the table SUBSTATION to gather information about the substations. 
var df_substation = get_substation(spark)

<!-- scala> df_substation.printSchema
root
 |-- substation_id: string (nullable = true)
 |-- substation_name: string (nullable = true)
 |-- state: string (nullable = true)
 |-- snapshot_year: string (nullable = true)
 |-- snapshot_month: string (nullable = true)
 |-- snapshot_day: string (nullable = true) -->


 <!-- scala> df_feeder.printSchema
root
 |-- feeder_id: string (nullable = true)
 |-- substation_bus_id: string (nullable = true)
 |-- substation_id: string (nullable = true)
 |-- state: string (nullable = true)
 |-- snapshot_year: string (nullable = true)
 |-- snapshot_month: string (nullable = true)
 |-- snapshot_day: string (nullable = true) -->

 var df_feeder_sub = df_feeder.join(df_substation, 
    df_substation("state") === df_feeder("state") &&
    df_substation("substation_id") === df_feeder("substation_id") &&
    df_substation("snapshot_year") === df_feeder("snapshot_year") &&
    df_substation("snapshot_month") === df_feeder("snapshot_month") &&
    df_substation("snapshot_day") === df_feeder("snapshot_day"), "left"
 ).select(
     df_feeder("state"),
     df_feeder("feeder_id"),
     df_feeder("substation_id"),
     df_feeder("snapshot_year"),
     df_feeder("snapshot_month"),
     df_feeder("snapshot_day")
     )

<!-- scala> df_feeder_sub.printSchema
root
 |-- state: string (nullable = true)
 |-- feeder_id: string (nullable = true)
 |-- substation_id: string (nullable = true)
 |-- snapshot_year: string (nullable = true)
 |-- snapshot_month: string (nullable = true)
 |-- snapshot_day: string (nullable = true) -->

 <!-- scala> df_smart_feeder.printSchema
root
 |-- state: string (nullable = true)
 |-- smart_meter_id: string (nullable = true)
 |-- transformer_id: string (nullable = true)
 |-- feeder_id: string (nullable = true)
 |-- snapshot_year: string (nullable = true)
 |-- snapshot_month: string (nullable = true)
 |-- snapshot_day: string (nullable = true) -->

 var df_smart_sub = df_smart_feeder.join(df_feeder_sub,
    df_feeder_sub("state")===df_smart_feeder("state") &&
    df_feeder_sub("feeder_id") === df_smart_feeder("feeder_id") &&
    df_feeder_sub("snapshot_year") === df_smart_feeder("snapshot_year") &&
    df_feeder_sub("snapshot_month") === df_smart_feeder("snapshot_month") &&
    df_feeder_sub("snapshot_day") === df_smart_feeder("snapshot_day"), "left").select(
    df_smart_feeder("state"),
    df_smart_feeder("smart_meter_id"),
    df_smart_feeder("transformer_id"),
    df_smart_feeder("feeder_id"),
    df_feeder_sub("substation_id"),
    when(df_smart_feeder("event_type") === "Over Voltage",1).otherwise(0).as("high_event"),
    when(df_smart_feeder("event_type") === "Under Voltage",1).otherwise(0).as("low_event"),
    df_smart_feeder("snapshot_year"),
    df_smart_feeder("snapshot_month"),
    df_smart_feeder("snapshot_day")
    )

<!-- scala> df_smart_sub.printSchema
root
 |-- state: string (nullable = true)
 |-- smart_meter_id: string (nullable = true)
 |-- transformer_id: string (nullable = true)
 |-- feeder_id: string (nullable = true)
 |-- substation_id: string (nullable = true)
 |-- event_type: string (nullable = true)
 |-- snapshot_year: string (nullable = true)
 |-- snapshot_month: string (nullable = true)
 |-- snapshot_day: string (nullable = true) -->

 var df_smart_sub_events =  df_smart_sub.groupBy(
    "state", 
    "snapshot_year", 
    "snapshot_month", 
    "snapshot_day", 
    "transformer_id", 
    "feeder_id", 
    "substation_id").agg(
    sum("high_event").as("high_event_sum"),
    sum("low_event").as("low_event_sum"))