var df_smart_meter_ct = get_smart_meter(spark).filter("state='CT'")

var df_dates = allDatesFromDate("2018-07-10", -30)

// CROSS JOIN THE SMART METER DATA AND PAST WEEK DATES
var df_smart_week_ct = df_smart_meter_ct.select(col("smart_meter_id")).crossJoin(df_dates.toDF("events_year", "events_month", "events_day"))

<!-- root
 |-- smart_meter_id: string (nullable = true)
 |-- events_year: string (nullable = true)
 |-- events_month: string (nullable = true)
 |-- events_day: string (nullable = true) -->


// GET OUT OF VOLTAGE EVENTS FOR CT
var df_ooevents_ct = get_out_of_voltage_event(spark).filter("state='CT'")

// JOIN IT WITH THE DATES 
df_ooevents_ct =  df_ooevents_ct.join(df_dates, 
       df_dates("year") === df_ooevents_ct("event_start_year") &&
       df_dates("month") === df_ooevents_ct("event_start_month") &&
       df_dates("day") === df_ooevents_ct("event_start_day"),"inner")

       // GROUP BY AND AGG 
var df_ooevents_daily_ct = df_ooevents_ct.groupBy("smart_meter_id", "event_start_year", "event_start_month", "event_start_day").agg(count("smart_meter_id").as("event_count"))

// JOIN WITH WEEK DAYS SELECTED 
var df_smart_days_ct = df_smart_week_ct.join(df_ooevents_daily_ct, df_smart_week_ct("smart_meter_id") === df_ooevents_daily_ct("smart_meter_id") &&
df_smart_week_ct("events_year") === df_ooevents_daily_ct("event_start_year") &&
df_smart_week_ct("events_month") === df_ooevents_daily_ct("event_start_month") &&
df_smart_week_ct("events_day") === df_ooevents_daily_ct("event_start_day"), "left").select(
df_smart_week_ct("smart_meter_id"), 
df_smart_week_ct("events_year"), 
df_smart_week_ct("events_month"),
df_smart_week_ct("events_day"), 
when(df_ooevents_daily_ct("smart_meter_id").isNotNull, 1).otherwise(0).as("has_events"))