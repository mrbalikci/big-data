// IMPORT DEPENDENCIES 
import java.time.LocalDate
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.date_format
import org.apache.spark.sql.functions._

// FUNCTION THAT CREATE PREVIOUS 7 DAYS
def allDatesFromDate(start_date: String , days:Long): DataFrame =   {
    val pDays:Long = math.abs(days)

    var rootDate:LocalDate = LocalDate.parse(start_date)
    //manage negative days
    if (days < 0 )
      rootDate = LocalDate.parse(start_date).minusDays(pDays+1)

    val dateSequence : Seq[String] =
      for( iDays <- 1 to pDays.asInstanceOf[Int] )
        yield rootDate.plusDays(iDays).toString

    dateSequence.toDF("proc_date").select(
      date_format(col("proc_date"),"YYYY").as("YEAR"),
      date_format(col("proc_date"),"MM").as("MONTH"),
      date_format(col("proc_date"),"dd").as("DAY")
     )
  }


// GET SMART METER DATA FOR CT 
// FILTER STATE >> PARAMETERIZE IT 
var df_smart_meter = get_smart_meter(spark).filter("state='CT'")

// FILTER DEVICE
<!-- df_smart_meter.filter("snapshot_day='1'") -->

// GET PAST WEEK DATES 
// USE PAST DAYS FUNCTION
var df_dates = allDatesFromDate("2018-06-10",-7)

// CROSS JOIN THE SMART METER DATA AND PAST WEEK DATES
var df_smart_week = df_smart_meter.select(col("smart_meter_id")).crossJoin(df_dates.toDF("events_year", "events_month", "events_day"))

<!-- root
 |-- smart_meter_id: string (nullable = true)
 |-- events_year: string (nullable = true)
 |-- events_month: string (nullable = true)
 |-- events_day: string (nullable = true) -->


// GET OUT OF VOLTAGE EVENTS FOR CT
var df_ooevents = get_out_of_voltage_event(spark).filter("state='CT'")

// JOIN IT WITH THE DATES 
df_ooevents =  df_ooevents.join(df_dates, 
       df_dates("year") === df_ooevents("event_start_year") &&
       df_dates("month") === df_ooevents("event_start_month") &&
       df_dates("day") === df_ooevents("event_start_day"),"inner")

// GROUP BY AND AGG
var df_ooevents_daily = df_ooevents.groupBy("smart_meter_id", "event_start_year", "event_start_month", "event_start_day").agg(count("smart_meter_id").as("event_count"))

// JOIN WITH WEEK DAYS SELECTED 
var df_smart_days = df_smart_week.join(df_ooevents_daily, df_smart_week("smart_meter_id") === df_ooevents_daily("smart_meter_id") &&
df_smart_week("events_year") === df_ooevents_daily("event_start_year") &&
df_smart_week("events_month") === df_ooevents_daily("event_start_month") &&
df_smart_week("events_day") === df_ooevents_daily("event_start_day"), "left").select(
df_smart_week("smart_meter_id"), 
df_smart_week("events_year"), 
df_smart_week("events_month"),
df_smart_week("events_day"), 
when(df_ooevents_daily("smart_meter_id").isNotNull, 1).otherwise(0).as("has_events"))

// AGG THE SUM OF EVENTS
// NUMBERS SHOULD BE BETWEEN 0-7
var df_smart_days_with_events =  df_smart_days.groupBy("smart_meter_id").agg(sum("has_events").as("days_with_event"))

// OBSERVE THE ACCURACY 
df_smart_days_with_events.select("days_with_event").distinct

// GET THE PERSISTENCE SEVERITY 
var ind = spark.sql("select * from avangrid_dev2.indicator_severity_levels where state = 'CT' and indicator_name='PERSISTANCE_FACTOR'")

// APPLY INDICATOR SEVERITY 
var df_event = df_smart_days_with_events.join(ind, 
    df_smart_days_with_events("days_with_event") > ind("indicator_lower_bound") && df_smart_days_with_events("days_with_event") <= ind("indicator_higher_bound"), "inner").select(
        df_smart_days_with_events("smart_meter_id"),
        df_smart_days_with_events("days_with_event"),
        ind("indicator_severity")
    )

<!-- scala> df_event.printSchema
root
 |-- smart_meter_id: string (nullable = true)
 |-- days_with_event: long (nullable = true)
 |-- indicator_severity: string (nullable = true) -->

// PHASE A 

<!-- scala> df_ooevents.printSchema
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
 |-- event_start_day: string (nullable = true)
 |-- YEAR: string (nullable = true)
 |-- MONTH: string (nullable = true)
 |-- DAY: string (nullable = true) -->

// GET THE REFERENCE VOLTAGE 
// READ SMART METER
var df_meter = spark.sql("select * from avangrid_dev2.smart_meter")

// GET SMART METER ID AND REFERENCE VOLTAGE 

df_meter = df_meter.select("smart_meter_id", "reference_voltage_a", "reference_voltage_b", "reference_voltage_c")

// JOIN REFERENCE VOLTAGE WITH OUT OF EVENTS DATA 

var df_ooevents_final = df_ooevents.join(df_meter,df_meter("smart_meter_id") === df_ooevents("smart_meter_id"), "inner").select(df_ooevents("smart_meter_id"), 
                                    df_ooevents("state"),
                                    df_ooevents("event_start_time"), 
                                    df_ooevents("event_max_voltage"), 
                                    df_ooevents("event_min_voltage"), 
                                    df_ooevents("event_mean_voltage"), 
                                    df_ooevents("event_voltage_phase"), 
                                    df_ooevents("event_start_year"),
                                    df_ooevents("event_start_month"),
                                    df_ooevents("event_start_day"),
                                    df_meter("reference_voltage_a"),
                                    df_meter("reference_voltage_b"),
                                    df_meter("reference_voltage_c"))

// FILTER BASED ON PHASE
// PHASE A

var df_ooevents_final_a = df_ooevents_final.filter("event_voltage_phase='Phase A'")


// GET RATIOS

df_ooevents_final_a = df_ooevents_final_a.withColumn("min_ratio_a", col("event_min_voltage")/col("reference_voltage_a")).withColumn("max_ratio_a", col("event_max_voltage")/col("reference_voltage_a")).withColumn("mean_ratio_a", col("event_mean_voltage")/col("reference_voltage_a"))

<!-- df_ooevents_final_a.withColumn("max_ratio_a", col("event_max_voltage")/col("reference_voltage_a"))

df_ooevents_final_a.withColumn("mean_ratio_a", col("event_mean_voltage")/col("reference_voltage_a")) -->


var df_phase_a = df_ooevents_final_a.select(
    col("smart_meter_id"),
    col("state"),
    col("event_min_voltage").as("event_min_voltage_a"),
    col("event_max_voltage").as("event_max_voltage_a"),
    col("min_ratio_a"),
    col("max_ratio_a"),
    col("mean_ratio_a"),
    col("event_voltage_phase"),
    col("event_start_year"),
    col("event_start_month"),
    col("event_start_day")
)


// FILTER BASED ON PHASE
// PHASE B

var df_ooevents_final_b = df_ooevents_final.filter("event_voltage_phase='Phase B'")


// GET RATIOS

df_ooevents_final_b = df_ooevents_final_b.withColumn("min_ratio_b", col("event_min_voltage")/col("reference_voltage_b")).withColumn("max_ratio_b", col("event_max_voltage")/col("reference_voltage_b")).withColumn("mean_ratio_b", col("event_mean_voltage")/col("reference_voltage_b"))

<!-- df_ooevents_final_b.withColumn("max_ratio_b", col("event_max_voltage")/col("reference_voltage_b"))

df_ooevents_final_b.withColumn("mean_ratio_b", col("event_mean_voltage")/col("reference_voltage_b")) -->


var df_phase_b = df_ooevents_final_b.select(
    col("smart_meter_id"),
    col("state"),
    col("event_min_voltage").as("event_min_voltage_b"),
    col("event_max_voltage").as("event_max_voltage_b"),
    col("min_ratio_b"),
    col("max_ratio_b"),
    col("mean_ratio_b"),
    col("event_voltage_phase"),
    col("event_start_year"),
    col("event_start_month"),
    col("event_start_day")
)

// FILTER BASED ON PHASE
// PHASE C

var df_ooevents_final_c = df_ooevents_final.filter("event_voltage_phase='Phase C'")


// GET RATIOS

df_ooevents_final_c = df_ooevents_final_c.withColumn("min_ratio_c", col("event_min_voltage")/col("reference_voltage_c")).withColumn("max_ratio_c", col("event_max_voltage")/col("reference_voltage_c")).withColumn("mean_ratio_c", col("event_mean_voltage")/col("reference_voltage_c"))

<!-- df_ooevents_final_c.withColumn("max_ratio_c", col("event_max_voltage")/col("reference_voltage_c"))

df_ooevents_final_c.withColumn("mean_ratio_c", col("event_mean_voltage")/col("reference_voltage_c")) -->


var df_phase_c = df_ooevents_final_c.select(
    col("smart_meter_id"),
    col("state"),
    col("event_min_voltage").as("event_min_voltage_c"),
    col("event_max_voltage").as("event_max_voltage_c"),
    col("min_ratio_c"),
    col("max_ratio_c"),
    col("mean_ratio_c"),
    col("event_voltage_phase"),
    col("event_start_year"),
    col("event_start_month"),
    col("event_start_day")
)

// JOIN PHASES
var df_phase_a_b = df_phase_a.join(df_phase_b, 
    df_phase_a("smart_meter_id")===df_phase_b("smart_meter_id") &&
    df_phase_a("state")===df_phase_b("state") &&
    df_phase_a("event_start_year")===df_phase_b("event_start_year") &&
    df_phase_a("event_start_month")===df_phase_b("event_start_month") &&
    df_phase_a("event_start_day")===df_phase_b("event_start_day"), "left").select(
        df_phase_a("smart_meter_id"),
        df_phase_a("state"),
        df_phase_a("event_min_voltage_a"),
        df_phase_a("event_max_voltage_a"),
        df_phase_a("min_ratio_a"),
        df_phase_a("max_ratio_a"),
        df_phase_a("mean_ratio_a"), 
        df_phase_b("event_min_voltage_b"),
        df_phase_b("event_max_voltage_b"),
        df_phase_b("min_ratio_b"),
        df_phase_b("max_ratio_b"),
        df_phase_b("mean_ratio_b"),
        df_phase_a("event_start_year"),
        df_phase_a("event_start_month"),
        df_phase_a("event_start_day"))

<!-- var df_phase_a_b_c = df_phase_a.join(df_phase_b, 
    df_phase_a("smart_meter_id")===df_phase_b("smart_meter_id") &&
    df_phase_a("state")===df_phase_b("state") &&
    df_phase_a("event_start_year")===df_phase_b("event_start_year") &&
    df_phase_a("event_start_month")===df_phase_b("event_start_month") &&
    df_phase_a("event_start_day")===df_phase_b("event_start_day"), "left").select(
        df_phase_a("smart_meter_id"),
        df_phase_a("event_min_voltage_a"),
        df_phase_a("event_max_voltage_a"),
        df_phase_a("min_ratio_a"),
        df_phase_a("max_ratio_a"),
        df_phase_a("mean_ratio_a"), 
        df_phase_b("event_min_voltage_b"),
        df_phase_b("event_max_voltage_b"),
        df_phase_b("min_ratio_b"),
        df_phase_b("max_ratio_b"),
        df_phase_b("mean_ratio_b"),
        df_phase_a("event_start_year"),
        df_phase_a("event_start_month"),
        df_phase_a("event_start_day")) -->


var df_phase_a_b_c = df_phase_a_b.join(df_phase_c, 
    df_phase_a_b("smart_meter_id")===df_phase_c("smart_meter_id") &&
    df_phase_a_b("state")===df_phase_c("state") &&
    df_phase_a_b("event_start_year")===df_phase_c("event_start_year") &&
    df_phase_a_b("event_start_month")===df_phase_c("event_start_month") &&
    df_phase_a_b("event_start_day")===df_phase_c("event_start_day"), "left").select(
        df_phase_a_b("smart_meter_id"),
        df_phase_a_b("state"),
        df_phase_a_b("event_min_voltage_a"),
        df_phase_a_b("event_max_voltage_a"),
        df_phase_a_b("min_ratio_a"),
        df_phase_a_b("max_ratio_a"),
        df_phase_a_b("mean_ratio_a"), 
        df_phase_a_b("event_min_voltage_b"),
        df_phase_a_b("event_max_voltage_b"),
        df_phase_a_b("min_ratio_b"),
        df_phase_a_b("max_ratio_b"),
        df_phase_a_b("mean_ratio_b"),
        df_phase_c("event_min_voltage_c"),
        df_phase_c("event_max_voltage_c"),
        df_phase_c("min_ratio_c"),
        df_phase_c("max_ratio_c"),
        df_phase_c("mean_ratio_c"),
        df_phase_a_b("event_start_year"),
        df_phase_a_b("event_start_month"),
        df_phase_a_b("event_start_day"))


<!-- scala> df_phase_a_b_c.printSchema
root
 |-- smart_meter_id: string (nullable = true)
 |-- event_min_voltage_a: double (nullable = true)
 |-- event_max_voltage_a: double (nullable = true)
 |-- min_ratio_a: double (nullable = true)
 |-- max_ratio_a: double (nullable = true)
 |-- mean_ratio_a: double (nullable = true)
 |-- event_min_voltage_b: double (nullable = true)
 |-- event_max_voltage_b: double (nullable = true)
 |-- min_ratio_b: double (nullable = true)
 |-- max_ratio_b: double (nullable = true)
 |-- mean_ratio_b: double (nullable = true)
 |-- event_min_voltage_c: double (nullable = true)
 |-- event_max_voltage_c: double (nullable = true)
 |-- min_ratio_c: double (nullable = true)
 |-- max_ratio_c: double (nullable = true)
 |-- mean_ratio_c: double (nullable = true)
 |-- event_start_year: string (nullable = true)
 |-- event_start_month: string (nullable = true)
 |-- event_start_day: string (nullable = true) -->

// GET 30 DAYS DATA 

// READ METERS
var df_smart_meter = get_smart_meter(spark).filter("state='CT'")

// PAST 30 DAYS 
var df_dates_30 = allDatesFromDate("2018-07-10",-30)

// GET OUT OF VOLTAGE DATA
// FILTER BY STATE
var df_ooevents = get_out_of_voltage_event(spark).filter("state='CT'")


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

var df_smart_week_30 = df_smart_meter.select(col("smart_meter_id")).crossJoin(df_dates_30.toDF("events_year","events_month","events_day"))

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

df_smart_days_count_events_30 = df_smart_days_count_events_30.withColumn("percent_days_high", col("high_events")/30).withColumn("percent_days_low", col("low_events")/30)


<!-- df_smart_days_count_events_30.select("high_events").distinct.show -->


// JOIN ALL 3 DATAFRAMES 


<!-- scala> df_event.printSchema
root
 |-- smart_meter_id: string (nullable = true)
 |-- days_with_event: long (nullable = true)
 |-- indicator_severity: string (nullable = true) -->


<!-- scala> df_phase_a_b_c.printSchema
root
 |-- smart_meter_id: string (nullable = true)
 |-- state: string (nullable = true)
 |-- event_min_voltage_a: double (nullable = true)
 |-- event_max_voltage_a: double (nullable = true)
 |-- min_ratio_a: double (nullable = true)
 |-- max_ratio_a: double (nullable = true)
 |-- mean_ratio_a: double (nullable = true)
 |-- event_min_voltage_b: double (nullable = true)
 |-- event_max_voltage_b: double (nullable = true)
 |-- min_ratio_b: double (nullable = true)
 |-- max_ratio_b: double (nullable = true)
 |-- mean_ratio_b: double (nullable = true)
 |-- event_min_voltage_c: double (nullable = true)
 |-- event_max_voltage_c: double (nullable = true)
 |-- min_ratio_c: double (nullable = true)
 |-- max_ratio_c: double (nullable = true)
 |-- mean_ratio_c: double (nullable = true)
 |-- event_start_year: string (nullable = true)
 |-- event_start_month: string (nullable = true)
 |-- event_start_day: string (nullable = true) -->


var df_smart_meter_daily = df_event.join(df_phase_a_b_c, 
    df_phase_a_b_c("smart_meter_id")===df_event("smart_meter_id"), "left"
).select(
    df_phase_a_b_c("smart_meter_id"),
    df_event("days_with_event").as("persistence_factor"),
    df_event("indicator_severity").as("persistence_severity"),
    df_phase_a_b_c("event_min_voltage_a").as("minimum_voltage_a"),
    df_phase_a_b_c("event_max_voltage_a").as("maximum_voltage_a"),
    df_phase_a_b_c("min_ratio_a").as("min_max_nominal_ratio_a"),
    df_phase_a_b_c("max_ratio_a").as("max_max_nominal_ratio_a"),
    df_phase_a_b_c("mean_ratio_a").as("mean_max_nominal_ratio_a"), 
    df_phase_a_b_c("event_min_voltage_b").as("minimum_voltage_b"),
    df_phase_a_b_c("event_max_voltage_b").as("maximum_voltage_b"),
    df_phase_a_b_c("min_ratio_b").as("min_max_nominal_ratio_b"),
    df_phase_a_b_c("max_ratio_b").as("max_max_nominal_ratio_b"),
    df_phase_a_b_c("mean_ratio_b").as("mean_max_nominal_ratio_b"), 
    df_phase_a_b_c("event_min_voltage_c").as("minimum_voltage_c"),
    df_phase_a_b_c("event_max_voltage_c").as("maximum_voltage_c"),
    df_phase_a_b_c("min_ratio_c").as("min_max_nominal_ratio_c"),
    df_phase_a_b_c("max_ratio_c").as("max_max_nominal_ratio_c"),
    df_phase_a_b_c("mean_ratio_c").as("mean_max_nominal_ratio_c"),
    df_phase_a_b_c("state"),
    df_phase_a_b_c("event_start_year").as("indicator_year"),
    df_phase_a_b_c("event_start_month").as("indicator_month"),
    df_phase_a_b_c("event_start_day").as("indicator_day"))


<!-- scala> df_smart_meter_daily.printSchema
root
 |-- smart_meter_id: string (nullable = true)
 |-- persistence_factor: long (nullable = true)
 |-- persistence_severity: string (nullable = true)
 |-- minimum_voltage_a: double (nullable = true)
 |-- maximum_voltage_a: double (nullable = true)
 |-- min_max_nominal_ratio_a: double (nullable = true)
 |-- max_max_nominal_ratio_a: double (nullable = true)
 |-- mean_max_nominal_ratio_a: double (nullable = true)
 |-- minimum_voltage_b: double (nullable = true)
 |-- maximum_voltage_b: double (nullable = true)
 |-- min_max_nominal_ratio_b: double (nullable = true)
 |-- max_max_nominal_ratio_b: double (nullable = true)
 |-- mean_max_nominal_ratio_b: double (nullable = true)
 |-- minimum_voltage_c: double (nullable = true)
 |-- maximum_voltage_c: double (nullable = true)
 |-- min_max_nominal_ratio_c: double (nullable = true)
 |-- max_max_nominal_ratio_c: double (nullable = true)
 |-- mean_max_nominal_ratio_c: double (nullable = true)
 |-- state: string (nullable = true)
 |-- indicator_year: string (nullable = true)
 |-- indicator_month: string (nullable = true)
 |-- indicator_day: string (nullable = true) -->


 <!-- scala> df_smart_days_count_events_30.printSchema
root
 |-- smart_meter_id: string (nullable = true)
 |-- high_events: long (nullable = true)
 |-- low_events: long (nullable = true)
 |-- percent_days_high: double (nullable = true)
 |-- percent_days_low: long (nullable = true) -->

var df_smart_meter_daily_indicators = df_smart_meter_daily.join(df_smart_days_count_events_30, df_smart_days_count_events_30("smart_meter_id")===df_smart_meter_daily("smart_meter_id"), "left").select(
    df_smart_meter_daily("smart_meter_id"),
    df_smart_meter_daily("persistence_factor"),
    df_smart_meter_daily("persistence_severity"), 
    df_smart_meter_daily("minimum_voltage_a"),
    df_smart_meter_daily("maximum_voltage_a"),
    df_smart_meter_daily("min_max_nominal_ratio_a"),
    df_smart_meter_daily("max_max_nominal_ratio_a"),
    df_smart_meter_daily("mean_max_nominal_ratio_a"),
    df_smart_meter_daily("minimum_voltage_b"),
    df_smart_meter_daily("maximum_voltage_b"),
    df_smart_meter_daily("min_max_nominal_ratio_b"),  
    df_smart_meter_daily("max_max_nominal_ratio_b"),
    df_smart_meter_daily("mean_max_nominal_ratio_b"),
    df_smart_meter_daily("minimum_voltage_c"),
    df_smart_meter_daily("maximum_voltage_c"),
    df_smart_meter_daily("min_max_nominal_ratio_c"),  
    df_smart_meter_daily("max_max_nominal_ratio_c"),
    df_smart_meter_daily("mean_max_nominal_ratio_c"),
    df_smart_days_count_events_30("percent_days_high").as("percent_days_oov_high"),
    df_smart_days_count_events_30("percent_days_low").as("percent_days_oov_low"),
    df_smart_days_count_events_30("high_events"),
    df_smart_days_count_events_30("low_events"),
    df_smart_meter_daily("state"),
    df_smart_meter_daily("indicator_year"),
    df_smart_meter_daily("indicator_month"),
    df_smart_meter_daily("indicator_day")
)

<!-- scala> df_smart_meter_daily_indicators.printSchema
root
 |-- smart_meter_id: string (nullable = true)
 |-- persistence_factor: long (nullable = true)
 |-- persistence_severity: string (nullable = true)
 |-- minimum_voltage_a: double (nullable = true)
 |-- maximum_voltage_a: double (nullable = true)
 |-- min_max_nominal_ratio_a: double (nullable = true)
 |-- max_max_nominal_ratio_a: double (nullable = true)
 |-- mean_max_nominal_ratio_a: double (nullable = true)
 |-- minimum_voltage_b: double (nullable = true)
 |-- maximum_voltage_b: double (nullable = true)
 |-- min_max_nominal_ratio_b: double (nullable = true)
 |-- max_max_nominal_ratio_b: double (nullable = true)
 |-- mean_max_nominal_ratio_b: double (nullable = true)
 |-- minimum_voltage_c: double (nullable = true)
 |-- maximum_voltage_c: double (nullable = true)
 |-- min_max_nominal_ratio_c: double (nullable = true)
 |-- max_max_nominal_ratio_c: double (nullable = true)
 |-- mean_max_nominal_ratio_c: double (nullable = true)
 |-- percent_days_oov_high: double (nullable = true)
 |-- percent_days_oov_low: long (nullable = true)
 |-- high_events: long (nullable = true)
 |-- low_events: long (nullable = true)
 |-- state: string (nullable = true)
 |-- indicator_year: string (nullable = true)
 |-- indicator_month: string (nullable = true)
 |-- indicator_day: string (nullable = true) -->