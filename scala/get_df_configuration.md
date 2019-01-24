package org.avangrid.analytics.config
package org.avangrid.analytics.config
import org.apache.spark.SparkConf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}

spark.conf.set("spark.sql.crossJoin.enabled", "true")


var df_conf = get_event_mapping(spark)
df_conf.show

// apply the filters 
var conf_filter = " COALESCE(event_mapping_effective_date, CURRENT_TIMESTAMP) <= CURRENT_TIMESTAMP AND COALESCE(event_mapping_expiration_date, CURRENT_TIMESTAMP) >= CURRENT_TIMESTAMP"

// Filter for smart meter
df_conf = df_conf.filter(conf_filter)
df_conf = df_conf.filter("device_type='smart_meter'")

// Get the start 
 var df_start = df_notifications.join(df_conf, df_conf("state") === df_notifications("state")
      && df_conf("notification_start_message") === df_notifications("notification_description")
      && df_conf("device_type") === df_notifications("device_type")
      ,"inner").select(
      df_notifications("device_id"),
      df_notifications("notification_description").as("event_description"),
      df_conf("device_type"),
      df_conf("event_type"),
      df_notifications("notification_time").as("start_time"),
      df_notifications("state")).distinct

// See if NY data is there 
df_start.select(col("state")).distinct.show

