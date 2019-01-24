import java.time.LocalDate

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.date_format
import org.apache.spark.sql.functions._




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

