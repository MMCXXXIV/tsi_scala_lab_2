package lab2

import org.apache.spark.sql.DataFrame

// Saves the dataset as CSV files in the given folder
object Task10SaveData {
  def save(df: DataFrame, path: String): Unit = {
    df.write
      .mode("overwrite")
      .option("header", "true")
      .csv(path)
  }
}
