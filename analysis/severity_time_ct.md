// Time Severity Conditions

df_event = df_event.withColumn("criticality", when(col("time_diff").between(0.0,15.0),"LOW") && when(col("time_diff").between(15.0,60.0),"MEDIUM") && when(col("time_diff").between(60.0,360.0),"HIGH") && when(col("time_diff").between(360.0,1.0E11),"HIGH"))

df_event = df_event.withColumn("criticality", when(col("time_diff").between(15.0,60.0),"MEDIUM")).otherwise(df_event("time_diff")))

df_event.withColumn("criticality", when($"time_diff"<=15.0, "LOW").otherwise("MEDIUM")).show()

df_event.withColumn("criticality", when($"time_diff"<=15.0, "LOW").otherwise(when($"time_diff" <= 60.0, "MEDIUM"))).withColumn("criticality", when($"time_diff"<=360.0, "HIGHT").otherwise(when($"time_diff" > 560.0, "VERY HIGH"))).show()

df_event = df_event.withColumn("criticality1", when(col("time_diff").between(0.0,15.0),"LOW").otherwise(when(col("time_diff").between(15.0,60.0),"MEDIUM")))

df_event = df_event.withColumn("time_severity", when(col("time_diff").between(60.0,360.0),"HIGH").otherwise(when(col("time_diff").between(60.0,1.0E11),"VERY HIGH")))

df_event = df_event.withColumn("time_severity", coalesce($"criticality1", $"criticality2")).drop("criticality1").drop("criticality2")



