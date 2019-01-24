- MM/DD/YY format into YYYY-MM-DD format

val temp = df_min.withColumn("reading_dt", from_unixtime(unix_timestamp(col("reading_dt"), "MM/dd/yy"), "yyyy-MM-dd"))

- 