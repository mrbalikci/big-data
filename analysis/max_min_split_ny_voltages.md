1. Import the reading 
var df = spark.sql("select * from avangrid_dev.reading")

2. Filter the NY state
df = df.filter("state='NY'")

3. Import dependencies to split max and min based on the index: odd/even situation
import org.apache.spark.sql.expressions._

4. Define a var for even 
var n = 2 

5. Filter index ODDs as the first value 
var df_min = df.withColumn("index", row_number().over(Window.orderBy(monotonically_increasing_id))).filter($"index" % n !== 0)

6. Select appropirate columns for df_min
df_min = df_min.select(col("reading_time").as("reading_time_start"), col("reading_value").as("reading_value_start"), col("reading_unit"), col("device_id"), col("state"), col("device_type"), col("reading_type"), col("reading_dt"))

7. Filter index EVENs as the second value
var df_max = df.withColumn("index", row_number().over(Window.orderBy(monotonically_increasing_id))).filter($"index" % n === 0)

8. Select appropirate columns for df_max 
df_max = df_max.select(col("reading_time").as("reading_time_end"), col("reading_value").as("reading_value_end"), col("device_id"))

9. Join two DFs
var df_final = df_max.join(df_min, df_max("device_id") === df_min("device_id"))

10. Order the columns 
var df_final = df_final.select(col("reading_time_start"), col("reading_time_end"), col("reading_value_start"), col("reading_value_end"), col("device_id"), col("reading_unit"), col("state"), col("device_type"), col("reading_type"), col("reading_dt"))