package lab2

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

// Sorts the data by date, starting with the newest
object Task5SortByDate {
  def sort(df: DataFrame): DataFrame = {
    df.orderBy(col("date").desc)
  }
}
