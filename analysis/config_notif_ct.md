val db = "avangrid_dev." // database name TODO: parametrize
var state = "CT" //STATE 
//Filter to get only active reference voltages 
var conf_filter = "state='" + state + "' AND COALESCE(event_mapping_effective_date, CURRENT_TIMESTAMP) <= CURRENT_TIMESTAMP AND COALESCE(event_mapping_expiration_date, CURRENT_TIMESTAMP) >= CURRENT_TIMESTAMP"

var df_conf = spark.sql("select * from " + db + "event_mapping")

df_conf = df_conf.filter(conf_filter)
df_conf = df_conf.filter("device_type='smart_meter'")
df_conf.alias("conf")

var df_notifications = spark.sql("select * from avangrid_dev.notification")

//CT
df_notifications = df_notifications.filter("state='CT'")
df_notifications = df_notifications.filter("notification_dt='2018-06-01'")
df_notifications = df_notifications.filter("notification_type='Alarm'")


//arrange the start events
               
               
var df_start = df_notifications.join(df_conf, df_conf("state") === df_notifications("state") 
               && df_conf("notification_start_message") === df_notifications("notification_description") 
               && df_conf("device_type") === df_notifications("device_type")
               ,"inner").select(df_notifications("device_id"),
                                df_conf("device_type"),
                                df_conf("event_type"), 
                                df_notifications("notification_time").as("start_time")).distinct

var df_end = df_notifications.join(df_conf, df_conf("state") === df_notifications("state") 
               && df_conf("notification_end_message") === df_notifications("notification_description") 
               && df_conf("device_type") === df_notifications("device_type")
               ,"inner").select(df_notifications("device_id"),
                                df_conf("device_type"),
                                df_conf("event_type"), 
                                df_notifications("notification_time").as("end_time")).distinct
               
var df_event = df_start.join(df_end, df_start("device_id") === df_end("device_id")
                                   && df_start("event_type") === df_end("event_type")
                                   && df_start("device_type") === df_end("device_type")
                                   && df_end("end_time") > df_start("start_time")
                                   ,"left").select(df_start("device_id"),
                                                   df_start("device_type"),
                                                   df_start("event_type"),
                                                   df_start("start_time"),
                                                   df_end("end_time"))
                                                                                                                                                                                                   
df_event = df_event.groupBy("event_type","start_time","device_id").agg(min("end_time").as("end_time"))

df_event.show(10,false)
