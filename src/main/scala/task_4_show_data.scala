package lab2

import org.apache.spark.sql.DataFrame

// Shows the first five rows of the dataset
object Task4ShowData {
  def show(df: DataFrame): Unit = {
    df.show(5, truncate = false)
  }
}
