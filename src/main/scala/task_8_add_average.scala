package lab2

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

// Adds a new column with the average of DAX and FTSE
object Task8AddAverage {
  def add(df: DataFrame): DataFrame = {
    df.withColumn("DAX_FTSE_AVG", (col("DAX") + col("FTSE")) / 2.0)
  }
}
