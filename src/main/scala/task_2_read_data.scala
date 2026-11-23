package lab2

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

// Reads the CSV file and converts the date column to a date type
object Task2ReadData {
  def read(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
      .withColumn("date", col("date").cast("date"))
  }
}

