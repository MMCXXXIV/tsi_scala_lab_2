package lab2

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

// Sorts the NIKKEI values from highest to lowest
object Task7SortByNikkei {
  def sort(df: DataFrame): DataFrame = {
    df.orderBy(col("NIKKEI").desc)
  }
}
