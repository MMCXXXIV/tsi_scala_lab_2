package lab2

import org.apache.spark.sql.DataFrame

// Gets the column names and puts them into one string
object Task3PrintHeader {
  def print(df: DataFrame): String = {
    df.columns.mkString(", ")
  }
}
