package lab2

import org.apache.spark.sql.DataFrame

// Removes the EM column from the dataset
object Task6RemoveEM {
  def remove(df: DataFrame): DataFrame = {
    df.drop("EM")
  }
}
