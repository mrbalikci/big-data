// Step 1

df_event = df_event.withColumn("nominal_voltage", when(col("voltage").between(220,260),240).otherwise(120))

// Step 2 

df_event = df_event.withColumn("diff_voltages", abs(($"voltage")-($"nominal_voltage")))

// Step 3
df_event = df_event.select(col("device_id"), col("start_time"), col("end_time"), col("event_type"), col("voltage"), col("nominal_voltage"), col("time_diff"), col("criticality"),  bround(df_event("diff_voltages"),2).as("diff_in_voltage"))

// Step 4

df_event = df_event.withColumn("voltage_critic1", when(col("diff_in_voltage").between(0.0,0.05),"LOW").otherwise(when(col("diff_in_voltage").between(0.05,0.1),"MEDIUM")))

// Step 5

df_event = df_event.withColumn("voltage_critic2", when(col("diff_in_voltage").between(0.1,1.0),"HIGH")

// Step 6

df_event = df_event.withColumn("voltage_criticality", coalesce($"voltage_critic1", $"voltage_critic2")).drop("voltage_critic1").drop("voltage_critic2")

df_event.show()