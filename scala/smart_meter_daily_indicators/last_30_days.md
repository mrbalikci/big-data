// READ METERS
var df_smart_meter_ct = get_smart_meter(spark).filter("state='CT'")

// A MONTH 
var df_dates_30 = allDatesFromDate("2018-07-10",-30)

// 
var df_ooevents = get_out_of_voltage_event(spark).filter("state='CT'")
//add filter for date


var df_ooevents_30 =  df_ooevents.join(df_dates_30, 
       df_dates_30("year") === df_ooevents("event_start_year") &&
       df_dates_30("month") === df_ooevents("event_start_month") &&
       df_dates_30("day") === df_ooevents("event_start_day"),"inner").select(
col("smart_meter_id"),
col("event_start_year"),
col("event_start_month"),
col("event_start_day"),
when(col("event_type") === "Over Voltage",1).otherwise(0).as("Over Voltage"),
when(col("event_type") === "Under Voltage",1).otherwise(0).as("Under Voltage")
)

var df_ooevents_30_days =  df_ooevents_30.groupBy("smart_meter_id").agg(
    sum("Over Voltage").as("Over Voltage"),
    sum("Under Voltage").as("Under Voltage"))

var df_ooevents_daily_30 = df_ooevents_30.groupBy(
"smart_meter_id", 
"event_start_year", 
"event_start_month",
"event_start_day"
).agg(when(sum("Under Voltage")>0,1).otherwise(0).as("high_events_sum"),
      when(sum("Over Voltage")>0,1).otherwise(0).as("low_events_sum"))

var df_smart_week_30 = df_smart_meter_ct.select(col("smart_meter_id")).crossJoin(df_dates_30.toDF("events_year","events_month","events_day"))

var df_smart_days_30 = df_smart_week_30.join(df_ooevents_daily_30, df_smart_week_30("smart_meter_id") === df_ooevents_daily_30("smart_meter_id") &&
df_smart_week_30("events_year") === df_ooevents_daily_30("event_start_year") &&
df_smart_week_30("events_month") === df_ooevents_daily_30("event_start_month") &&
df_smart_week_30("events_day") === df_ooevents_daily_30("event_start_day"), "left").select(
df_smart_week_30("smart_meter_id"), 
df_smart_week_30("events_year"), 
df_smart_week_30("events_month"),
df_smart_week_30("events_day"), 
coalesce(df_ooevents_daily_30("high_events_sum"),lit(0)).as("high_events"),
coalesce(df_ooevents_daily_30("low_events_sum"),lit(0)).as("low_events")
)

var df_smart_days_count_events_30 =  df_smart_days_30.groupBy("smart_meter_id").agg(
    sum("high_events").as("high_events"), 
    sum("low_events").as("low_events"))

df_smart_days_count_events_30.withColumn("percent_days_high", col("high_events")/30).withColumn("percent_days_low", col("low_events"))


df_smart_days_count_events_30.select("high_events").distinct.show



