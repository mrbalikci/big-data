1. Import the data 
`var df = spark.sql("select * from avangrid_dev.notification")`

2. Filter by the data / TODO: paramatize 
`df = df.filter("notification_dt='2018-06-01'")`

3. See the data 
`df.show`

4. Select distinct colums for the events 
`var df_events  = df.select(col("notification_description"), col("state")).distinct`

5. See the data with details 
`df_events.show(100, false)`

6. Filter the over voltages 
`var df_over_vol = df.filter("notification_description = 'Diagnostic 7 Over Voltage, Phase A'")`

7. See the data 
`df_over_vol.show()`

8. Filter the over voltages cleared data 
`var df_over_vol_cleared = df.filter("notification_description = 'Diagnostic 7 Condition Cleared'")`

9. See the data 
`df_over_vol_cleared.show()`

10. Joint over voltages and over voltages cleared data based on smart meter id

11. Show the new joined data 

12. Filter the under voltage data 
`var df_under_vol = df.filter("notification_description = 'Diagnostic 6 UnderVoltage, Phase A'")`

13. See the data 
`df_under_vol.show()`

14. Filter the under voltage cleared data
`var df_under_vol_cleared = df.filter("notification_description = 'Diagnostic 6 Condition Cleared'")`

15. Join the under voltage and under voltage cleared data based on the smart meter id

`val join1 = df_under_vol.where(df_under_vol.col("notification_description").equalTo("Diagnostic 6 UnderVoltage, Phase A")).join(df_under_vol_cleared, df_under_vol.col("device_id") === df_under_vol_cleared("device_id"))`


.select("notification_time", "notification_code", "notification_description", "notification_long_description", "device_id", "state", "device_type", "notification_type", "notification_dt")


16. See the new joined data


- Schema 
root
 |-- notification_time: timestamp (nullable = true)
 |-- notification_code: string (nullable = true)
 |-- notification_description: string (nullable = true)
 |-- notification_long_description: string (nullable = true)
 |-- device_id: string (nullable = true)
 |-- state: string (nullable = true)
 |-- device_type: string (nullable = true)
 |-- notification_type: string (nullable = true)
 |-- notification_dt: string (nullable = true)


