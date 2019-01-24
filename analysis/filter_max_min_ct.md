var df = spark.sql("select * from avangrid_dev.notification")

//CT
df = df.filter("state='CT'")
df = df.filter("notification_dt='2018-06-01'")
df = df.filter("notification_type='Alarm'")

var df_start = df.filter ("notification_description = 'Diagnostic 7 Over Voltage, Phase A'").select(col("notification_description"),col("notification_time").as("start_time"), col("device_id").as("start_device_id"))

var df_end = df.filter ("notification_description = 'Diagnostic 7 Condition Cleared'").select(col("notification_time").as("end_time"), col("device_id").as("end_device_id"))

var df_event = df_start.join(df_end, df_start("start_device_id") === df_end("end_device_id") && df_end("end_time") > df_start("start_time"),"left")

df_event = df_event.groupBy("notification_description","start_time","start_device_id").agg(min("end_time").as("end_time"))

var df_vol = spark.sql("select * from avangrid_dev.reading")

df_vol = df_vol.filter("state='CT'")

df_vol =  df_vol.filter("reading_time='2018-06-01'")

var df_vol_event = df_vol.join(df_event, df_vol("device_id") === df_event("start_device_id") && df_vol("reading_dt") === to_date(df_event("start_time")), "inner")

// get the max values 
var df_vol_event_max = df_vol_event.groupBy("device_id", "start_time", "end_time", "notification_description").max("reading_value").as("reading_value")


