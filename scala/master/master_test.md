var df_ooevent = get_out_of_voltage_event(spark)

<!-- scala> df_ooevent.printSchema
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

 var df_smart_meter = get_smart_meter(spark)

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

 var smart_meter_indic = get_smart_meter_daily_indicators(spark)

<!-- scala> smart_meter_indic.printSchema
root
 |-- smart_meter_id: string (nullable = true)
 |-- smart_meter_persistence_factor: double (nullable = true)
 |-- smart_meter_persistence_factor_severity: string (nullable = true)
 |-- smart_meter_minimum_voltage_a: double (nullable = true)
 |-- smart_meter_maximum_voltage_a: double (nullable = true)
 |-- smart_meter_minimum_maximum_nominal_voltage_ratio_a: double (nullable = true)
 |-- smart_meter_maximum_maximum_nominal_voltage_ratio_a: double (nullable = true)
 |-- smart_meter_mean_maximum_nominal_voltage_ratio_a: double (nullable = true)
 |-- smart_meter_minimum_voltage_b: double (nullable = true)
 |-- smart_meter_maximum_voltage_b: double (nullable = true)
 |-- smart_meter_minimum_maximum_nominal_voltage_ratio_b: double (nullable = true)
 |-- smart_meter_maximum_maximum_nominal_voltage_ratio_b: double (nullable = true)
 |-- smart_meter_mean_maximum_nominal_voltage_ratio_b: double (nullable = true)
 |-- smart_meter_minimum_voltage_c: double (nullable = true)
 |-- smart_meter_maximum_voltage_c: double (nullable = true)
 |-- meter_minimum_maximum_nominal_voltage_ratio_c: double (nullable = true)
 |-- smart_meter_maximum_maximum_nominal_voltage_ratio_c: double (nullable = true)
 |-- smart_meter_mean_maximum_nominal_voltage_ratio_c: double (nullable = true)
 |-- smart_meter_percentage_of_days_out_of_voltage_high: double (nullable = true)
 |-- smart_meter_percentage_of_days_out_of_voltage_low: double (nullable = true)
 |-- smart_meter_high_events: double (nullable = true)
 |-- smart_meter_low_events: double (nullable = true)
 |-- state: string (nullable = true)
 |-- indicators_year: string (nullable = true)
 |-- indicators_month: string (nullable = true)
 |-- indicators_day: string (nullable = true) -->

 var smart_meters_join = smart_meter_indic.join(df_smart_meter,
    df_smart_meter("state")===smart_meter_indic("state") &&
    df_smart_meter("smart_meter_id")===smart_meter_indic("smart_meter_id") &&
    df_smart_meter("snapshot_year")===smart_meter_indic("indicators_year") &&
    df_smart_meter("snapshot_month")===smart_meter_indic("indicators_month") &&
    df_smart_meter("snapshot_day")===smart_meter_indic("indicators_day"), "inner").select(
    df_smart_meter("state"),
    df_smart_meter("snapshot_year"),
    df_smart_meter("snapshot_month"),
    df_smart_meter("snapshot_day"),
    df_smart_meter("smart_meter_id"),
    df_smart_meter("transformer_id"),
    df_smart_meter("transformer_bank_id"),
    df_smart_meter("transformer_mesh_id"),
    df_smart_meter("latitude").as("smart_meter_latitude"),
    df_smart_meter("longitude").as("smart_meter_longitude"),
    df_smart_meter("city").as("smart_meter_city"),
    df_smart_meter("address").as("smart_meter_address"),
    df_smart_meter("zipcode").as("smart_meter_zip_code"),
    df_smart_meter("revenue_class").as("smart_meter_revenue_class"),
    df_smart_meter("rate_plan").as("smart_meter_rate_plan"),
    df_smart_meter("der_customer").as("smart_meter_der_customer"),
    df_smart_meter("reference_voltage_a").as("smart_meter_reference_voltage_a"),
    df_smart_meter("reference_voltage_b").as("smart_meter_reference_voltage_b"),
    df_smart_meter("reference_voltage_c").as("smart_meter_reference_voltage_c"),
    smart_meter_indic("smart_meter_persistence_factor"),
    smart_meter_indic("smart_meter_persistence_factor_severity"),
    smart_meter_indic("smart_meter_minimum_voltage_a"),
    smart_meter_indic("smart_meter_maximum_voltage_a"),
    smart_meter_indic("smart_meter_minimum_maximum_nominal_voltage_ratio_a"),
    smart_meter_indic("smart_meter_maximum_maximum_nominal_voltage_ratio_a"),
    smart_meter_indic("smart_meter_mean_maximum_nominal_voltage_ratio_a"),
    smart_meter_indic("smart_meter_minimum_voltage_b"),
    smart_meter_indic("smart_meter_maximum_voltage_b"),
    smart_meter_indic("smart_meter_minimum_maximum_nominal_voltage_ratio_b"),
    smart_meter_indic("smart_meter_maximum_maximum_nominal_voltage_ratio_b"),
    smart_meter_indic("smart_meter_mean_maximum_nominal_voltage_ratio_b"),
    smart_meter_indic("smart_meter_minimum_voltage_c"),
    smart_meter_indic("smart_meter_maximum_voltage_c"),
    smart_meter_indic("meter_minimum_maximum_nominal_voltage_ratio_c"),
    smart_meter_indic("smart_meter_maximum_maximum_nominal_voltage_ratio_c"),
    smart_meter_indic("smart_meter_mean_maximum_nominal_voltage_ratio_c"),
    smart_meter_indic("smart_meter_percentage_of_days_out_of_voltage_high"),
    smart_meter_indic("smart_meter_percentage_of_days_out_of_voltage_low"),
    smart_meter_indic("smart_meter_high_events"),
    smart_meter_indic("smart_meter_low_events"))


// JOIN EVENT AND SMART METER JOIN 
var event_smart_meter_join = df_ooevent.join(smart_meters_join,
    smart_meters_join("state")===smart_meter_indic("state") &&
    smart_meters_join("smart_meter_id")===smart_meter_indic("smart_meter_id") &&
    smart_meters_join("snapshot_year")===smart_meter_indic("indicators_year") &&
    smart_meters_join("snapshot_month")===smart_meter_indic("indicators_month") &&
    smart_meters_join("snapshot_day")===smart_meter_indic("indicators_day"), "inner").select(
        df_ooevent("event_start_time"),
        df_ooevent("event_end_time"),
        df_ooevent("event_elapsed_time"),
        df_ooevent("event_description"),
        df_ooevent("event_voltage_phase"),
        df_ooevent("event_max_voltage"),
        df_ooevent("event_min_voltage"),
        df_ooevent("event_out_of_voltage_percentage"),
        df_ooevent("overall_severity"),
        df_ooevent("voltage_severity"),
        df_ooevent("event_type"),
        df_ooevent("time_severity"),
        smart_meters_join("state"),
        smart_meters_join("snapshot_year"),
        smart_meters_join("snapshot_month"),
        smart_meters_join("snapshot_day"),
        smart_meters_join("smart_meter_id"),
        smart_meters_join("transformer_id"),
        smart_meters_join("transformer_bank_id"),
        smart_meters_join("transformer_mesh_id"),
        smart_meters_join("smart_meter_latitude"),
        smart_meters_join("smart_meter_longitude"),
        smart_meters_join("smart_meter_city"),
        smart_meters_join("smart_meter_address"),
        smart_meters_join("smart_meter_zip_code"),
        smart_meters_join("smart_meter_revenue_class"),
        smart_meters_join("smart_meter_rate_plan"),
        smart_meters_join("smart_meter_der_customer"),
        smart_meters_join("smart_meter_reference_voltage_a"),
        smart_meters_join("smart_meter_reference_voltage_b"),
        smart_meters_join("smart_meter_reference_voltage_c"),
        smart_meters_join("smart_meter_persistence_factor"),
        smart_meters_join("smart_meter_persistence_factor_severity"),
        smart_meters_join("smart_meter_minimum_voltage_a"),
        smart_meters_join("smart_meter_maximum_voltage_a"),
        smart_meters_join("smart_meter_minimum_maximum_nominal_voltage_ratio_a"),
        smart_meters_join("smart_meter_maximum_maximum_nominal_voltage_ratio_a"),
        smart_meters_join("smart_meter_mean_maximum_nominal_voltage_ratio_a"),
        smart_meters_join("smart_meter_minimum_voltage_b"),
        smart_meters_join("smart_meter_maximum_voltage_b"),
        smart_meters_join("smart_meter_minimum_maximum_nominal_voltage_ratio_b"),
        smart_meters_join("smart_meter_maximum_maximum_nominal_voltage_ratio_b"),
        smart_meters_join("smart_meter_mean_maximum_nominal_voltage_ratio_b"),
        smart_meters_join("smart_meter_minimum_voltage_c"),
        smart_meters_join("smart_meter_maximum_voltage_c"),
        smart_meters_join("meter_minimum_maximum_nominal_voltage_ratio_c"),
        smart_meters_join("smart_meter_maximum_maximum_nominal_voltage_ratio_c"),
        smart_meters_join("smart_meter_mean_maximum_nominal_voltage_ratio_c"),
        smart_meters_join("smart_meter_percentage_of_days_out_of_voltage_high"),
        smart_meters_join("smart_meter_percentage_of_days_out_of_voltage_low"),
        smart_meters_join("smart_meter_high_events"),
        smart_meters_join("smart_meter_low_events"))

// JOIN TRANSFORMER AND TRANSFORMER INDICATOR 


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

 var df_trans_indic = get_transformer_daily_indicators(spark)

 <!-- scala> df_trans_indic.printSchema
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


var df_trans_join = df_transformer.join(df_trans_indic, 
    df_trans_indic("state")===df_transformer("state") &&
    df_trans_indic("transformer_id")===df_transformer("transformer_id") &&
    df_trans_indic("indicators_year")===df_transformer("snapshot_year") &&
    df_trans_indic("indicators_month")===df_transformer("snapshot_month") &&
    df_trans_indic("indicators_day")===df_transformer("snapshot_day"), "inner").select(
    df_transformer("transformer_id"),
    df_transformer("transformer_bank_id"),
    df_transformer("transformer_mesh_id"),
    df_transformer("transformer_feeder_id"),
    df_transformer("state"),
    df_transformer("snapshot_year"),
    df_transformer("snapshot_month"),
    df_transformer("snapshot_day"),
    df_transformer("latitude").as("transformer_latitude"),
    df_transformer("longitude").as("transformer_longitude"),
    df_transformer("transformer_phase"),
    df_transformer("transformer_size"),
    df_transformer("transformer_configuration"),
    df_transformer("transformer_low_end_configuration"),
    df_transformer("transformer_low_end_voltage"),
    df_transformer("transformer_high_end_configuration"),
    df_transformer("transformer_high_end_voltage"),
    df_transformer("transformer_installation_date"),
    df_transformer("transformer_manufacturer"),
    df_trans_indic("transformer_percentage_of_affected_customers"),
    df_trans_indic("transformer_percentage_of_affected_customers_severity"),
    df_trans_indic("transformer_customers"))

// JOIN EVENT SMART METER AND TRANSFORMER_JOIN

var df_event_smart_meter_trans = event_smart_meter_join.join(df_trans_join, 
    df_trans_join("state")===df_transformer("state") &&
    df_trans_join("transformer_id")===event_smart_meter_join("transformer_id") &&
    df_trans_join("indicators_year")===event_smart_meter_join("snapshot_year") &&
    df_trans_join("indicators_month")===event_smart_meter_join("snapshot_month") &&
    df_trans_join("indicators_day")===event_smart_meter_join("snapshot_day"), "inner").select(
        event_smart_meter_join("smart_meter_id"),
        df_trans_join("transformer_id"),
        df_trans_join("transformer_feeder_id"),
        df_trans_join("transformer_mesh_id"),
        df_trans_join("transformer_bank_id"),
        event_smart_meter_join("state"),
        event_smart_meter_join("snapshot_year"),
        event_smart_meter_join("snapshot_month"),
        event_smart_meter_join("snapshot_day"),
        event_smart_meter_join("event_start_time"),
        event_smart_meter_join("event_end_time"),
        event_smart_meter_join("event_elapsed_time"),
        event_smart_meter_join("event_description"),
        event_smart_meter_join("event_voltage_phase"),
        event_smart_meter_join("event_max_voltage"),
        event_smart_meter_join("event_min_voltage"),
        event_smart_meter_join("event_out_of_voltage_percentage"),
        event_smart_meter_join("overall_severity"),
        event_smart_meter_join("voltage_severity"),
        event_smart_meter_join("time_severity"),
        event_smart_meter_join("smart_meter_latitude"),
        event_smart_meter_join("smart_meter_longitude"),
        event_smart_meter_join("smart_meter_city"),
        event_smart_meter_join("smart_meter_address"),
        event_smart_meter_join("smart_meter_zip_code"),
        event_smart_meter_join("smart_meter_revenue_class"),
        event_smart_meter_join("smart_meter_rate_plan"),
        event_smart_meter_join("smart_meter_der_customer"),
        event_smart_meter_join("smart_meter_reference_voltage_a"),
        event_smart_meter_join("smart_meter_reference_voltage_b"),
        event_smart_meter_join("smart_meter_reference_voltage_c"),
        event_smart_meter_join("smart_meter_persistence_factor"),
        event_smart_meter_join("smart_meter_persistence_factor_severity"),
        event_smart_meter_join("smart_meter_minimum_voltage_a"),
        event_smart_meter_join("smart_meter_maximum_voltage_a"),
        event_smart_meter_join("smart_meter_minimum_maximum_nominal_voltage_ratio_a"),
        event_smart_meter_join("smart_meter_maximum_maximum_nominal_voltage_ratio_a"),
        event_smart_meter_join("smart_meter_mean_maximum_nominal_voltage_ratio_a"),
        event_smart_meter_join("smart_meter_minimum_voltage_b"),
        event_smart_meter_join("smart_meter_maximum_voltage_b"),
        event_smart_meter_join("smart_meter_minimum_maximum_nominal_voltage_ratio_b"),
        event_smart_meter_join("smart_meter_maximum_maximum_nominal_voltage_ratio_b"),
        event_smart_meter_join("smart_meter_mean_maximum_nominal_voltage_ratio_b"),
        event_smart_meter_join("smart_meter_minimum_voltage_c"),
        event_smart_meter_join("smart_meter_maximum_voltage_c"),
        event_smart_meter_join("meter_minimum_maximum_nominal_voltage_ratio_c"),
        event_smart_meter_join("smart_meter_maximum_maximum_nominal_voltage_ratio_c"),
        event_smart_meter_join("smart_meter_mean_maximum_nominal_voltage_ratio_c"),
        event_smart_meter_join("smart_meter_percentage_of_days_out_of_voltage_high"),
        event_smart_meter_join("smart_meter_percentage_of_days_out_of_voltage_low"),
        event_smart_meter_join("smart_meter_high_events"),
        event_smart_meter_join("smart_meter_low_events"),
        event_smart_meter_join("event_type"),
        df_trans_join("latitude").as("transformer_latitude"),
        df_trans_join("longitude").as("transformer_longitude"),
        df_trans_join("transformer_phase"),
        df_trans_join("transformer_size"),
        df_trans_join("transformer_configuration"),
        df_trans_join("transformer_low_end_configuration"),
        df_trans_join("transformer_low_end_voltage"),
        df_trans_join("transformer_high_end_configuration"),
        df_trans_join("transformer_high_end_voltage"),
        df_trans_join("transformer_installation_date"),
        df_trans_join("transformer_manufacturer"),
        df_trans_join("transformer_percentage_of_affected_customers"),
        df_trans_join("transformer_percentage_of_affected_customers_severity"),
        df_trans_join("transformer_customers"))

// JOIN FEEDER AND FEEDER INDICATOR 
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

 var df_feeder_indc = get_feeder_daily_indicators(spark)

 <!-- scala> df_feeder_indc.printSchema
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

var df_feeder_join = df_feeder_indc.join(df_feeder, 
    df_feeder("state")===df_feeder_indc("state") &&
    df_feeder("feeder_id")===df_feeder_indc("feeder_id") &&
    df_feeder("snapshot_year")===df_feeder_indc("indicators_year") &&
    df_feeder("snapshot_month")===df_feeder_indc("indicators_month") &&
    df_feeder("snapshot_day")===df_feeder_indc("indicators_day"), "inner").select(
        df_feeder("feeder_id"),
        df_feeder("substation_bus_id").as("feeder_substation_bus_id"),
        df_feeder("substation_id"),
        df_feeder("state"),
        df_feeder("snapshot_year"),
        df_feeder("snapshot_month"),
        df_feeder("snapshot_day"),
        df_feeder_indc("feeder_percentage_of_affected_customers"),
        df_feeder_indc("feeder_percentage_of_affected_customers_severity"),
        df_feeder_indc("feeder_customers"),
        df_feeder_indc("feeder_transformers"))


<!-- scala> df_feeder_join.printSchema
root
 |-- feeder_id: string (nullable = true)
 |-- feeder_substation_bus_id: string (nullable = true)
 |-- substation_id: string (nullable = true)
 |-- state: string (nullable = true)
 |-- snapshot_year: string (nullable = true)
 |-- snapshot_month: string (nullable = true)
 |-- snapshot_day: string (nullable = true)
 |-- feeder_percentage_of_affected_customers: double (nullable = true)
 |-- feeder_percentage_of_affected_customers_severity: double (nullable = true)
 |-- feeder_customers: double (nullable = true)
 |-- feeder_transformers: double (nullable = true) -->


 // JOIN EVENT SMART METER TRANSFORMER WITH FEEDER 

 df df_event_smart_meter_trans_feeder = df_event_smart_meter_trans.join(df_feeder_join, 
    df_feeder_join("state")===df_event_smart_meter_trans("state") &&
    df_feeder_join("feeder_id")===df_event_smart_meter_trans("transformer_feeder_id") &&
    df_feeder_join("snapshot_year")===df_event_smart_meter_trans("indicators_year") &&
    df_feeder_join("snapshot_month")===df_event_smart_meter_trans("indicators_month") &&
    df_feeder_join("snapshot_day")===df_event_smart_meter_trans("indicators_day"), "inner").select(
        df_event_smart_meter_trans("smart_meter_id"),
        df_event_smart_meter_trans("transformer_id"),
        df_feeder_join("feeder_id"),
        df_feeder_join("substation_id"),
        df_event_smart_meter_trans("transformer_mesh_id"),
        df_event_smart_meter_trans("transformer_bank_id"),
        df_event_smart_meter_trans("state"),
        df_event_smart_meter_trans("snapshot_year"),
        df_event_smart_meter_trans("snapshot_month"),
        df_event_smart_meter_trans("snapshot_day"),
        df_event_smart_meter_trans("event_start_time"),
        df_event_smart_meter_trans("event_end_time"),
        df_event_smart_meter_trans("event_elapsed_time"),
        df_event_smart_meter_trans("event_description"),
        df_event_smart_meter_trans("event_voltage_phase"),
        df_event_smart_meter_trans("event_max_voltage"),
        df_event_smart_meter_trans("event_min_voltage"),
        df_event_smart_meter_trans("event_out_of_voltage_percentage"),
        df_event_smart_meter_trans("overall_severity"),
        df_event_smart_meter_trans("voltage_severity"),
        df_event_smart_meter_trans("time_severity"),
        df_event_smart_meter_trans("smart_meter_latitude"),
        df_event_smart_meter_trans("smart_meter_longitude"),
        df_event_smart_meter_trans("smart_meter_city"),
        df_event_smart_meter_trans("smart_meter_address"),
        df_event_smart_meter_trans("smart_meter_zip_code"),
        df_event_smart_meter_trans("smart_meter_revenue_class"),
        df_event_smart_meter_trans("smart_meter_rate_plan"),
        df_event_smart_meter_trans("smart_meter_der_customer"),
        df_event_smart_meter_trans("smart_meter_reference_voltage_a"),
        df_event_smart_meter_trans("smart_meter_reference_voltage_b"),
        df_event_smart_meter_trans("smart_meter_reference_voltage_c"),
        df_event_smart_meter_trans("smart_meter_persistence_factor"),
        df_event_smart_meter_trans("smart_meter_persistence_factor_severity"),
        df_event_smart_meter_trans("smart_meter_minimum_voltage_a"),
        df_event_smart_meter_trans("smart_meter_maximum_voltage_a"),
        df_event_smart_meter_trans("smart_meter_minimum_maximum_nominal_voltage_ratio_a"),
        df_event_smart_meter_trans("smart_meter_maximum_maximum_nominal_voltage_ratio_a"),
        df_event_smart_meter_trans("smart_meter_mean_maximum_nominal_voltage_ratio_a"),
        df_event_smart_meter_trans("smart_meter_minimum_voltage_b"),
        df_event_smart_meter_trans("smart_meter_maximum_voltage_b"),
        df_event_smart_meter_trans("smart_meter_minimum_maximum_nominal_voltage_ratio_b"),
        df_event_smart_meter_trans("smart_meter_maximum_maximum_nominal_voltage_ratio_b"),
        df_event_smart_meter_trans("smart_meter_mean_maximum_nominal_voltage_ratio_b"),
        df_event_smart_meter_trans("smart_meter_minimum_voltage_c"),
        df_event_smart_meter_trans("smart_meter_maximum_voltage_c"),
        df_event_smart_meter_trans("meter_minimum_maximum_nominal_voltage_ratio_c"),
        df_event_smart_meter_trans("smart_meter_maximum_maximum_nominal_voltage_ratio_c"),
        df_event_smart_meter_trans("smart_meter_mean_maximum_nominal_voltage_ratio_c"),
        df_event_smart_meter_trans("smart_meter_percentage_of_days_out_of_voltage_high"),
        df_event_smart_meter_trans("smart_meter_percentage_of_days_out_of_voltage_low"),
        df_event_smart_meter_trans("smart_meter_high_events"),
        df_event_smart_meter_trans("smart_meter_low_events"),
        df_event_smart_meter_trans("latitude").as("transformer_latitude"),
        df_event_smart_meter_trans("longitude").as("transformer_longitude"),
        df_event_smart_meter_trans("transformer_phase"),
        df_event_smart_meter_trans("transformer_size"),
        df_event_smart_meter_trans("transformer_configuration"),
        df_event_smart_meter_trans("transformer_low_end_configuration"),
        df_event_smart_meter_trans("transformer_low_end_voltage"),
        df_event_smart_meter_trans("transformer_high_end_configuration"),
        df_event_smart_meter_trans("transformer_high_end_voltage"),
        df_event_smart_meter_trans("transformer_installation_date"),
        df_event_smart_meter_trans("transformer_manufacturer"),
        df_event_smart_meter_trans("transformer_percentage_of_affected_customers"),
        df_event_smart_meter_trans("transformer_percentage_of_affected_customers_severity"),
        df_event_smart_meter_trans("transformer_customers"),
        df_event_smart_meter_trans("event_type"),
        df_feeder_join("feeder_substation_bus_id"),   
        df_feeder_join("feeder_percentage_of_affected_customers"),
        df_feeder_join("feeder_percentage_of_affected_customers_severity"), 
        df_feeder_join("feeder_customers"),
        df_feeder_join("feeder_transformers"))

// JOIN SUBSTATION AND SUBSTATION INDICATOR 

var df_substation = get_substation(spark)

<!-- scala> df_substation.printSchema
root
 |-- substation_id: string (nullable = true)
 |-- substation_name: string (nullable = true)
 |-- state: string (nullable = true)
 |-- snapshot_year: string (nullable = true)
 |-- snapshot_month: string (nullable = true)
 |-- snapshot_day: string (nullable = true) -->

 var df_substation_indc = get_substation_daily_indicators(spark)

 <!-- scala> df_substation_indc.printSchema
root
 |-- substation_id: string (nullable = true)
 |-- substation_percentage_of_affected_customers: double (nullable = true)
 |-- substation_percentage_of_affected_customers_severity: string (nullable = true)
 |-- substation_high_events: double (nullable = true)
 |-- substation_low_events: double (nullable = true)
 |-- substation_customers: double (nullable = true)
 |-- substation_transformers: double (nullable = true)
 |-- substation_feeders: double (nullable = true)
 |-- substation_affected_customers: double (nullable = true)
 |-- substation_affected_transformers: double (nullable = true)
 |-- substation_affected_feeders: double (nullable = true)
 |-- state: string (nullable = true)
 |-- indicators_year: string (nullable = true)
 |-- indicators_month: string (nullable = true)
 |-- indicators_day: string (nullable = true) -->

 var df_substation_join = df_substation_indc.join(df_substation, 
    df_substation("state")===df_substation_indc("state") &&
    df_substation("substation_id")===df_substation_indc("substation_id") &&
    df_substation("snapshot_year")===df_substation_indc("indicators_year") &&
    df_substation("snapshot_month")===df_substation_indc("indicators_month") &&
    df_substation("snapshot_day")===df_substation_indc("indicators_day"), "inner").select(
        df_substation("substation_id"),
        df_substation("substation_name"),
        df_substation("state"),
        df_substation("snapshot_year"),
        df_substation("snapshot_month"),
        df_substation("snapshot_day"),
        df_substation_indc("substation_percentage_of_affected_customers"),
        df_substation_indc("substation_percentage_of_affected_customers_severity"),
        df_substation_indc("substation_customers"),
        df_substation_indc("substation_transformers"),
        df_substation_indc("substation_feeders"))


<!-- scala> df_substation_join.printSchema
root
 |-- substation_id: string (nullable = true)
 |-- substation_name: string (nullable = true)
 |-- state: string (nullable = true)
 |-- snapshot_year: string (nullable = true)
 |-- snapshot_month: string (nullable = true)
 |-- snapshot_day: string (nullable = true)
 |-- substation_percentage_of_affected_customers: double (nullable = true)
 |-- substation_percentage_of_affected_customers_severity: string (nullable = true)
 |-- substation_customers: double (nullable = true)
 |-- substation_transformers: double (nullable = true)
 |-- substation_feeders: double (nullable = true) -->



// JOIN EVENT SMART METER TRANS FEEDER WITH SUBSTATION 

var df_event_smart_meter_trans_feeder_subs = df_event_smart_meter_trans_feeder.join(df_substation_join,
    df_substation("state")===df_substation_join("state") &&
    df_substation_join("substation_id")===df_event_smart_meter_trans_feeder("substation_id") &&
    df_substation_join("snapshot_year")===df_event_smart_meter_trans_feeder("snapshot_year") &&
    df_substation_join("snapshot_month")===df_event_smart_meter_trans_feeder("snapshot_month") &&
    df_substation_join("snapshot_day")===df_event_smart_meter_trans_feeder("snapshot_day"), "inner").select(
        df_event_smart_meter_trans_feeder("smart_meter_id"),
        df_event_smart_meter_trans_feeder("transformer_id"),
        df_event_smart_meter_trans_feeder("feeder_id"),
        df_substation_join("substation_id"),
        df_event_smart_meter_trans_feeder("transformer_mesh_id"),
        df_event_smart_meter_trans_feeder("transformer_bank_id"),
        df_event_smart_meter_trans_feeder("state"),
        df_event_smart_meter_trans_feeder("snapshot_year"),
        df_event_smart_meter_trans_feeder("snapshot_month"),
        df_event_smart_meter_trans_feeder("snapshot_day"),
        df_event_smart_meter_trans_feeder("event_start_time"),
        df_event_smart_meter_trans_feeder("event_end_time"),
        df_event_smart_meter_trans_feeder("event_elapsed_time"),
        df_event_smart_meter_trans_feeder("event_description"),
        df_event_smart_meter_trans_feeder("event_voltage_phase"),
        df_event_smart_meter_trans_feeder("event_max_voltage"),
        df_event_smart_meter_trans_feeder("event_min_voltage"),
        df_event_smart_meter_trans_feeder("event_out_of_voltage_percentage"),
        df_event_smart_meter_trans_feeder("overall_severity"),
        df_event_smart_meter_trans_feeder("voltage_severity"),
        df_event_smart_meter_trans_feeder("time_severity"),
        df_event_smart_meter_trans_feeder("smart_meter_latitude"),
        df_event_smart_meter_trans_feeder("smart_meter_longitude"),
        df_event_smart_meter_trans_feeder("smart_meter_city"),
        df_event_smart_meter_trans_feeder("smart_meter_address"),
        df_event_smart_meter_trans_feeder("smart_meter_zip_code"),
        df_event_smart_meter_trans_feeder("smart_meter_revenue_class"),
        df_event_smart_meter_trans_feeder("smart_meter_rate_plan"),
        df_event_smart_meter_trans_feeder("smart_meter_der_customer"),
        df_event_smart_meter_trans_feeder("smart_meter_reference_voltage_a"),
        df_event_smart_meter_trans_feeder("smart_meter_reference_voltage_b"),
        df_event_smart_meter_trans_feeder("smart_meter_reference_voltage_c"),
        df_event_smart_meter_trans_feeder("smart_meter_persistence_factor"),
        df_event_smart_meter_trans_feeder("smart_meter_persistence_factor_severity"),
        df_event_smart_meter_trans_feeder("smart_meter_minimum_voltage_a"),
        df_event_smart_meter_trans_feeder("smart_meter_maximum_voltage_a"),
        df_event_smart_meter_trans_feeder("smart_meter_minimum_maximum_nominal_voltage_ratio_a"),
        df_event_smart_meter_trans_feeder("smart_meter_maximum_maximum_nominal_voltage_ratio_a"),
        df_event_smart_meter_trans_feeder("smart_meter_mean_maximum_nominal_voltage_ratio_a"),
        df_event_smart_meter_trans_feeder("smart_meter_minimum_voltage_b"),
        df_event_smart_meter_trans_feeder("smart_meter_maximum_voltage_b"),
        df_event_smart_meter_trans_feeder("smart_meter_minimum_maximum_nominal_voltage_ratio_b"),
        df_event_smart_meter_trans_feeder("smart_meter_maximum_maximum_nominal_voltage_ratio_b"),
        df_event_smart_meter_trans_feeder("smart_meter_mean_maximum_nominal_voltage_ratio_b"),
        df_event_smart_meter_trans_feeder("smart_meter_minimum_voltage_c"),
        df_event_smart_meter_trans_feeder("smart_meter_maximum_voltage_c"),
        df_event_smart_meter_trans_feeder("meter_minimum_maximum_nominal_voltage_ratio_c"),
        df_event_smart_meter_trans_feeder("smart_meter_maximum_maximum_nominal_voltage_ratio_c"),
        df_event_smart_meter_trans_feeder("smart_meter_mean_maximum_nominal_voltage_ratio_c"),
        df_event_smart_meter_trans_feeder("smart_meter_percentage_of_days_out_of_voltage_high"),
        df_event_smart_meter_trans_feeder("smart_meter_percentage_of_days_out_of_voltage_low"),
        df_event_smart_meter_trans_feeder("smart_meter_high_events"),
        df_event_smart_meter_trans_feeder("smart_meter_low_events"),
        df_event_smart_meter_trans_feeder("latitude").as("transformer_latitude"),
        df_event_smart_meter_trans_feeder("longitude").as("transformer_longitude"),
        df_event_smart_meter_trans_feeder("transformer_phase"),
        df_event_smart_meter_trans_feeder("transformer_size"),
        df_event_smart_meter_trans_feeder("transformer_configuration"),
        df_event_smart_meter_trans_feeder("transformer_low_end_configuration"),
        df_event_smart_meter_trans_feeder("transformer_low_end_voltage"),
        df_event_smart_meter_trans_feeder("transformer_high_end_configuration"),
        df_event_smart_meter_trans_feeder("transformer_high_end_voltage"),
        df_event_smart_meter_trans_feeder("transformer_installation_date"),
        df_event_smart_meter_trans_feeder("transformer_manufacturer"),
        df_event_smart_meter_trans_feeder("transformer_percentage_of_affected_customers"),
        df_event_smart_meter_trans_feeder("transformer_percentage_of_affected_customers_severity"),
        df_event_smart_meter_trans_feeder("transformer_customers"),
        df_event_smart_meter_trans_feeder("feeder_substation_bus_id"),   
        df_event_smart_meter_trans_feeder("feeder_percentage_of_affected_customers"),
        df_event_smart_meter_trans_feeder("feeder_percentage_of_affected_customers_severity"), 
        df_event_smart_meter_trans_feeder("feeder_customers"),
        df_event_smart_meter_trans_feeder("feeder_transformers"),
        df_event_smart_meter_trans_feeder("event_type"),
        df_substation_join("substation_name"),
        df_substation_join("state"),
        df_substation_join("snapshot_year"),
        df_substation_join("snapshot_month"),
        df_substation_join("snapshot_day"),
        df_substation_join_indc("substation_percentage_of_affected_customers"),
        df_substation_join_indc("substation_percentage_of_affected_customers_severity"),
        df_substation_join_indc("substation_customers"),
        df_substation_join_indc("substation_transformers"),
        df_substation_join_indc("substation_feeders"))

// GET MESH NETWORK 
var df_mesh = get_mesh_daily_indicators(spark)

<!-- scala> df_mesh.printSchema
root
 |-- mesh_network_id: string (nullable = true)
 |-- mesh_network_percentage_of_affected_customers: double (nullable = true)
 |-- mesh_network_percentage_of_affected_customers_severity: string (nullable = true)
 |-- mesh_network_high_events: double (nullable = true)
 |-- mesh_network_low_events: double (nullable = true)
 |-- mesh_network_customers: double (nullable = true)
 |-- mesh_network_transformers: double (nullable = true)
 |-- mesh_network_feeders: double (nullable = true)
 |-- mesh_network_affected_customers: double (nullable = true)
 |-- mesh_network_affected_transformers: double (nullable = true)
 |-- mesh_network_affected_feeders: double (nullable = true)
 |-- state: string (nullable = true)
 |-- indicators_year: string (nullable = true)
 |-- indicators_month: string (nullable = true)
 |-- indicators_day: string (nullable = true) -->


// JOIN EVENT SMART METER TRANS FEEDER SUBS WITH MESH 
 var df_event_smart_meter_trans_feeder_subs_mesh = df_event_smart_meter_trans_feeder_subs.join(df_mesh,
    df_mesh("state")===df_event_smart_meter_trans_feeder_subs("state") &&
    df_mesh("mesh_network_id")===df_event_smart_meter_trans_feeder_subs("transformer_mesh_id") &&
    df_mesh("indicators_year")===df_event_smart_meter_trans_feeder_subs("snapshot_year") &&
    df_mesh("indicators_month")===df_event_smart_meter_trans_feeder_subs("snapshot_month") &&
    df_mesh("indicators_day")===df_event_smart_meter_trans_feeder_subs("snapshot_day"), "left").select(
        df_event_smart_meter_trans_feeder_subs("smart_meter_id"),
        df_event_smart_meter_trans_feeder_subs("transformer_id"),
        df_event_smart_meter_trans_feeder_subs("feeder_id"),
        df_event_smart_meter_trans_feeder_subs("substation_id"),
        df_mesh("mesh_network_id"),
        df_event_smart_meter_trans_feeder_subs("transformer_bank_id"),
        df_event_smart_meter_trans_feeder_subs("state"),
        df_event_smart_meter_trans_feeder_subs("snapshot_year"),
        df_event_smart_meter_trans_feeder_subs("snapshot_month"),
        df_event_smart_meter_trans_feeder_subs("snapshot_day"),
        df_event_smart_meter_trans_feeder_subs("event_start_time"),
        df_event_smart_meter_trans_feeder_subs("event_end_time"),
        df_event_smart_meter_trans_feeder_subs("event_elapsed_time"),
        df_event_smart_meter_trans_feeder_subs("event_description"),
        df_event_smart_meter_trans_feeder_subs("event_voltage_phase"),
        df_event_smart_meter_trans_feeder_subs("event_max_voltage"),
        df_event_smart_meter_trans_feeder_subs("event_min_voltage"),
        df_event_smart_meter_trans_feeder_subs("event_out_of_voltage_percentage"),
        df_event_smart_meter_trans_feeder_subs("overall_severity"),
        df_event_smart_meter_trans_feeder_subs("voltage_severity"),
        df_event_smart_meter_trans_feeder_subs("time_severity"),
        df_event_smart_meter_trans_feeder_subs("smart_meter_latitude"),
        df_event_smart_meter_trans_feeder_subs("smart_meter_longitude"),
        df_event_smart_meter_trans_feeder_subs("smart_meter_city"),
        df_event_smart_meter_trans_feeder_subs("smart_meter_address"),
        df_event_smart_meter_trans_feeder_subs("smart_meter_zip_code"),
        df_event_smart_meter_trans_feeder_subs("smart_meter_revenue_class"),
        df_event_smart_meter_trans_feeder_subs("smart_meter_rate_plan"),
        df_event_smart_meter_trans_feeder_subs("smart_meter_der_customer"),
        df_event_smart_meter_trans_feeder_subs("smart_meter_reference_voltage_a"),
        df_event_smart_meter_trans_feeder_subs("smart_meter_reference_voltage_b"),
        df_event_smart_meter_trans_feeder_subs("smart_meter_reference_voltage_c"),
        df_event_smart_meter_trans_feeder_subs("smart_meter_persistence_factor"),
        df_event_smart_meter_trans_feeder_subs("smart_meter_persistence_factor_severity"),
        df_event_smart_meter_trans_feeder_subs("smart_meter_minimum_voltage_a"),
        df_event_smart_meter_trans_feeder_subs("smart_meter_maximum_voltage_a"),
        df_event_smart_meter_trans_feeder_subs("smart_meter_minimum_maximum_nominal_voltage_ratio_a"),
        df_event_smart_meter_trans_feeder_subs("smart_meter_maximum_maximum_nominal_voltage_ratio_a"),
        df_event_smart_meter_trans_feeder_subs("smart_meter_mean_maximum_nominal_voltage_ratio_a"),
        df_event_smart_meter_trans_feeder_subs("smart_meter_minimum_voltage_b"),
        df_event_smart_meter_trans_feeder_subs("smart_meter_maximum_voltage_b"),
        df_event_smart_meter_trans_feeder_subs("smart_meter_minimum_maximum_nominal_voltage_ratio_b"),
        df_event_smart_meter_trans_feeder_subs("smart_meter_maximum_maximum_nominal_voltage_ratio_b"),
        df_event_smart_meter_trans_feeder_subs("smart_meter_mean_maximum_nominal_voltage_ratio_b"),
        df_event_smart_meter_trans_feeder_subs("smart_meter_minimum_voltage_c"),
        df_event_smart_meter_trans_feeder_subs("smart_meter_maximum_voltage_c"),
        df_event_smart_meter_trans_feeder_subs("meter_minimum_maximum_nominal_voltage_ratio_c"),
        df_event_smart_meter_trans_feeder_subs("smart_meter_maximum_maximum_nominal_voltage_ratio_c"),
        df_event_smart_meter_trans_feeder_subs("smart_meter_mean_maximum_nominal_voltage_ratio_c"),
        df_event_smart_meter_trans_feeder_subs("smart_meter_percentage_of_days_out_of_voltage_high"),
        df_event_smart_meter_trans_feeder_subs("smart_meter_percentage_of_days_out_of_voltage_low"),
        df_event_smart_meter_trans_feeder_subs("smart_meter_high_events"),
        df_event_smart_meter_trans_feeder_subs("smart_meter_low_events"),
        df_event_smart_meter_trans_feeder_subs("latitude").as("transformer_latitude"),
        df_event_smart_meter_trans_feeder_subs("longitude").as("transformer_longitude"),
        df_event_smart_meter_trans_feeder_subs("transformer_phase"),
        df_event_smart_meter_trans_feeder_subs("transformer_size"),
        df_event_smart_meter_trans_feeder_subs("transformer_configuration"),
        df_event_smart_meter_trans_feeder_subs("transformer_low_end_configuration"),
        df_event_smart_meter_trans_feeder_subs("transformer_low_end_voltage"),
        df_event_smart_meter_trans_feeder_subs("transformer_high_end_configuration"),
        df_event_smart_meter_trans_feeder_subs("transformer_high_end_voltage"),
        df_event_smart_meter_trans_feeder_subs("transformer_installation_date"),
        df_event_smart_meter_trans_feeder_subs("transformer_manufacturer"),
        df_event_smart_meter_trans_feeder_subs("transformer_percentage_of_affected_customers"),
        df_event_smart_meter_trans_feeder_subs("transformer_percentage_of_affected_customers_severity"),
        df_event_smart_meter_trans_feeder_subs("transformer_customers"),
        df_event_smart_meter_trans_feeder_subs("feeder_substation_bus_id"),   
        df_event_smart_meter_trans_feeder_subs("feeder_percentage_of_affected_customers"),
        df_event_smart_meter_trans_feeder_subs("feeder_percentage_of_affected_customers_severity"), 
        df_event_smart_meter_trans_feeder_subs("feeder_customers"),
        df_event_smart_meter_trans_feeder_subs("feeder_transformers"),
        df_event_smart_meter_trans_feeder_subs("substation_name"),
        df_event_smart_meter_trans_feeder_subs("state"),
        df_event_smart_meter_trans_feeder_subs("snapshot_year"),
        df_event_smart_meter_trans_feeder_subs("snapshot_month"),
        df_event_smart_meter_trans_feeder_subs("snapshot_day"),
        df_event_smart_meter_trans_feeder_subs("substation_percentage_of_affected_customers"),
        df_event_smart_meter_trans_feeder_subs("substation_percentage_of_affected_customers_severity"),
        df_event_smart_meter_trans_feeder_subs("substation_customers"),
        df_event_smart_meter_trans_feeder_subs("substation_transformers"),
        df_event_smart_meter_trans_feeder_subs("substation_feeders"),
        df_event_smart_meter_trans_feeder_subs("event_type"),
        df_mesh("mesh_network_percentage_of_affected_customers"),
        df_mesh("mesh_network_percentage_of_affected_customers_severity"),
        df_mesh("mesh_network_customers"),
        df_mesh("mesh_network_transformers"),
        df_mesh("mesh_network_feeders"))

// GET TRANSFORMER BANK 

var df_bank = get_transformer_bank_daily_indicator(spark)

<!-- scala> df_bank.printSchema
root
 |-- transformer_bank_id: string (nullable = true)
 |-- transformer_bank_network_percentage_of_affected_customers: double (nullable = true)
 |-- transformer_bank_network_percentage_of_affected_customers_severity: string (nullable = true)
 |-- transformer_bank_high_events: double (nullable = true)
 |-- transformer_bank_low_events: double (nullable = true)
 |-- transformer_bank_customers: double (nullable = true)
 |-- transformer_bank_transformers: double (nullable = true)
 |-- transformer_bank_feeders: double (nullable = true)
 |-- transformer_bank_affected_customers: double (nullable = true)
 |-- transformer_bank_affected_transformers: double (nullable = true)
 |-- transformer_bank_affected_feeders: double (nullable = true)
 |-- state: string (nullable = true)
 |-- indicators_year: string (nullable = true)
 |-- indicators_month: string (nullable = true)
 |-- indicators_day: string (nullable = true) -->

 // JOIN EVENT SMART METER TRANS FEEDER SUBS MESH WITH BANK 

var df_event_smart_meter_trans_feeder_subs_mesh_bank = df_event_smart_meter_trans_feeder_subs_mesh.join(
    df_bank,
    df_bank("state")===df_event_smart_meter_trans_feeder_subs_mesh("state") &&
    df_bank("transformer_bank_id")===df_event_smart_meter_trans_feeder_subs_mesh("transformer_bank_id") &&
    df_bank("indicators_year")===df_event_smart_meter_trans_feeder_subs_mesh("snapshot_year") &&
    df_bank("indicators_month")===df_event_smart_meter_trans_feeder_subs_mesh("snapshot_month") &&
    df_bank("indicators_day")===df_event_smart_meter_trans_feeder_subs_mesh("snapshot_day"), "left").select(
        df_event_smart_meter_trans_feeder_subs_mesh("event_start_time"),
        df_event_smart_meter_trans_feeder_subs_mesh("event_end_time"),
        df_event_smart_meter_trans_feeder_subs_mesh("event_elapsed_time"),
        df_event_smart_meter_trans_feeder_subs_mesh("event_description"),
        df_event_smart_meter_trans_feeder_subs_mesh("event_voltage_phase"),
        df_event_smart_meter_trans_feeder_subs_mesh("event_max_voltage"),
        df_event_smart_meter_trans_feeder_subs_mesh("event_min_voltage"),
        df_event_smart_meter_trans_feeder_subs_mesh("event_out_of_voltage_percentage"),
        df_event_smart_meter_trans_feeder_subs_mesh("overall_severity"),
        df_event_smart_meter_trans_feeder_subs_mesh("voltage_severity"),
        df_event_smart_meter_trans_feeder_subs_mesh("time_severity"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_id"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_latitude"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_longitude"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_city"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_address"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_zip_code"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_revenue_class"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_rate_plan"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_der_customer"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_reference_voltage_a"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_reference_voltage_b"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_reference_voltage_c"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_persistence_factor"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_persistence_factor_severity"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_minimum_voltage_a"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_maximum_voltage_a"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_minimum_maximum_nominal_voltage_ratio_a"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_maximum_maximum_nominal_voltage_ratio_a"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_mean_maximum_nominal_voltage_ratio_a"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_minimum_voltage_b"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_maximum_voltage_b"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_minimum_maximum_nominal_voltage_ratio_b"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_maximum_maximum_nominal_voltage_ratio_b"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_mean_maximum_nominal_voltage_ratio_b"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_minimum_voltage_c"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_maximum_voltage_c"),
        df_event_smart_meter_trans_feeder_subs_mesh("meter_minimum_maximum_nominal_voltage_ratio_c"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_maximum_maximum_nominal_voltage_ratio_c"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_mean_maximum_nominal_voltage_ratio_c"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_percentage_of_days_out_of_voltage_high"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_percentage_of_days_out_of_voltage_low"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_high_events"),
        df_event_smart_meter_trans_feeder_subs_mesh("smart_meter_low_events"),
        df_event_smart_meter_trans_feeder_subs_mesh("transformer_id"),
        df_event_smart_meter_trans_feeder_subs_mesh("latitude").as("transformer_latitude"),
        df_event_smart_meter_trans_feeder_subs_mesh("longitude").as("transformer_longitude"),
        df_event_smart_meter_trans_feeder_subs_mesh("transformer_phase"),
        df_event_smart_meter_trans_feeder_subs_mesh("transformer_size"),
        df_event_smart_meter_trans_feeder_subs_mesh("transformer_configuration"),
        df_event_smart_meter_trans_feeder_subs_mesh("transformer_low_end_configuration"),
        df_event_smart_meter_trans_feeder_subs_mesh("transformer_low_end_voltage"),
        df_event_smart_meter_trans_feeder_subs_mesh("transformer_high_end_configuration"),
        df_event_smart_meter_trans_feeder_subs_mesh("transformer_high_end_voltage"),
        df_event_smart_meter_trans_feeder_subs_mesh("transformer_installation_date"),
        df_event_smart_meter_trans_feeder_subs_mesh("transformer_manufacturer"),
        df_event_smart_meter_trans_feeder_subs_mesh("transformer_percentage_of_affected_customers"),
        df_event_smart_meter_trans_feeder_subs_mesh("transformer_percentage_of_affected_customers_severity"),
        df_event_smart_meter_trans_feeder_subs_mesh("transformer_customers"),
        df_event_smart_meter_trans_feeder_subs_mesh("feeder_id"),
        df_event_smart_meter_trans_feeder_subs_mesh("feeder_substation_bus_id"),   
        df_event_smart_meter_trans_feeder_subs_mesh("feeder_percentage_of_affected_customers"),
        df_event_smart_meter_trans_feeder_subs_mesh("feeder_percentage_of_affected_customers_severity"),
        df_event_smart_meter_trans_feeder_subs_mesh("feeder_customers"),
        df_event_smart_meter_trans_feeder_subs_mesh("feeder_transformers"),
        df_event_smart_meter_trans_feeder_subs_mesh("substation_id"),
        df_event_smart_meter_trans_feeder_subs_mesh("substation_name"),
        df_event_smart_meter_trans_feeder_subs_mesh("substation_percentage_of_affected_customers"),
        df_event_smart_meter_trans_feeder_subs_mesh("substation_percentage_of_affected_customers_severity"),
        df_event_smart_meter_trans_feeder_subs_mesh("substation_customers"),
        df_event_smart_meter_trans_feeder_subs_mesh("substation_transformers"),
        df_event_smart_meter_trans_feeder_subs_mesh("substation_feeders"),
        df_event_smart_meter_trans_feeder_subs_mesh("mesh_network_id"),
        df_event_smart_meter_trans_feeder_subs_mesh("mesh_network_percentage_of_affected_customers"),
        df_event_smart_meter_trans_feeder_subs_mesh("mesh_network_percentage_of_affected_customers_severity"),
        df_event_smart_meter_trans_feeder_subs_mesh("mesh_network_customers"),
        df_event_smart_meter_trans_feeder_subs_mesh("mesh_network_transformers"),
        df_event_smart_meter_trans_feeder_subs_mesh("mesh_network_feeders"),
        df_bank("transformer_bank_id"),
        df_bank("transformer_bank_network_percentage_of_affected_customers"),
        df_bank("transformer_bank_network_percentage_of_affected_customers_severity"),
        df_bank("transformer_bank_customers"),
        df_bank("transformer_bank_transformers"),
        df_bank("transformer_bank_feeders"),
        df_event_smart_meter_trans_feeder_subs_mesh("state"),
        df_event_smart_meter_trans_feeder_subs_mesh("event_type"),
        df_event_smart_meter_trans_feeder_subs_mesh("snapshot_year"),
        df_event_smart_meter_trans_feeder_subs_mesh("snapshot_month"),
        df_event_smart_meter_trans_feeder_subs_mesh("snapshot_day"))