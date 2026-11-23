package lab2

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, max}

// Finds the highest value in the selected column
object Task9HighestValue {
  def highest(df: DataFrame, column: String): Double = {
    df.select(max(col(column)))
      .first()
      .getDouble(0)
  }
}
