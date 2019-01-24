package org.avangrid.analytics.config
package org.avangrid.analytics.config
import org.apache.spark.SparkConf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}

spark.conf.set("spark.sql.crossJoin.enabled", "true")

// NO OVERRIDE

var db = "avangrid_dev2"

 def get_ct_cis(ss: SparkSession): DataFrame = {
    ss.table(db + ".ct_cis")
  }
   def get_ct_daily_voltage(ss: SparkSession): DataFrame = {
    ss.table(db + ".ct_daily_voltage")
  }
   def get_ct_gis(ss: SparkSession): DataFrame = {
    ss.table(db + ".ct_gis")
  }
   def get_ct_notification(ss: SparkSession): DataFrame = {
    ss.table(db + ".ct_notification")
  }
   def get_ct_pm(ss: SparkSession): DataFrame = {
    ss.table(db + ".ct_pm")
  }

   def get_ny_cis(ss: SparkSession): DataFrame = {
    ss.table(db + ".ny_cis")
  }
   def get_ny_gis(ss: SparkSession): DataFrame = {
    ss.table(db + ".ny_gis")
  }
   def get_ny_notification(ss: SparkSession): DataFrame = {
    ss.table(db + ".ny_notification")
  }
   def get_ny_pm(ss: SparkSession): DataFrame = {
    ss.table(db + ".ny_pm")
  }
   def get_ny_smart_meter_configuration(ss: SparkSession): DataFrame = {
    ss.table(db + ".ny_smart_meter_configuration")
  }
   def get_ny_voltage_measures(ss: SparkSession): DataFrame = {
    ss.table(db + ".ny_voltage_measures")
  }

   def get_smart_meter(ss: SparkSession): DataFrame = {
    ss.table(db + ".smart_meter")
  }
   def get_transformer(ss: SparkSession): DataFrame = {
    ss.table(db + ".transformer")
  }
   def get_transformer_bank(ss: SparkSession): DataFrame = {
    ss.table(db + ".transformer_bank")
  }
   def get_feeder(ss: SparkSession): DataFrame = {
    ss.table(db + ".feeder")
  }
   def get_mesh_grid(ss: SparkSession): DataFrame = {
    ss.table(db + ".mesh_grid")
  }
   def get_substation(ss: SparkSession): DataFrame = {
    ss.table(db + ".substation")
  }

   def get_notification(ss: SparkSession): DataFrame = {
    ss.table(db + ".notification")
  }
   def get_reading(ss: SparkSession): DataFrame = {
    ss.table(db + ".reading")
  }

   def get_event_mapping(ss: SparkSession): DataFrame = {
    ss.table(db + ".event_mapping")
  }
   def get_indicator_severity_levels(ss: SparkSession): DataFrame = {
    ss.table(db + ".indicator_severity_levels")
  }
   def get_overall_out_of_voltage_severity_levels(ss: SparkSession): DataFrame = {
    ss.table(db + ".overall_out_of_voltage_severity_levels")
  }
   def get_reference_voltage_mapping(ss: SparkSession): DataFrame = {
    ss.table(db + ".reference_voltage_mapping")
  }
   def get_single_notifications(ss: SparkSession): DataFrame = {
    ss.table(db + ".single_notifications")
  }


   def get_out_of_voltage_event(ss: SparkSession): DataFrame = {
    ss.table(db + ".out_of_voltage_event")
  }
   def get_smart_meter_daily_indicators(ss: SparkSession): DataFrame = {
    ss.table(db + ".smart_meter_daily_indicators")
  }
   def get_transformer_bank_daily_indicator(ss: SparkSession): DataFrame = {
    ss.table(db + ".transformer_bank_daily_indicator")
  }
   def get_transformer_daily_indicators(ss: SparkSession): DataFrame = {
    ss.table(db + ".transformer_daily_indicators")
  }
   def get_feeder_daily_indicators(ss: SparkSession): DataFrame = {
    ss.table(db + ".feeder_daily_indicators")
  }
   def get_substation_daily_indicators(ss: SparkSession): DataFrame = {
    ss.table(db + ".substation_daily_indicators")
  }
   def get_mesh_daily_indicators(ss: SparkSession): DataFrame = {
    ss.table(db + ".mesh_daily_indicators")
  }
   def get_smart_meter_out_of_voltage_master_event(ss: SparkSession): DataFrame = {
    ss.table(db + ".smart_meter_out_of_voltage_master_event")
  }
   def get_smart_meter_hourly_voltage_reading(ss: SparkSession): DataFrame = {
    ss.table(db + ".smart_meter_hourly_voltage_reading")
  }