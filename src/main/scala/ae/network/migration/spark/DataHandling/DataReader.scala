package ae.network.migration.spark.DataHandling

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataReader {

  /**
   * Reads a CSV file into a DataFrame.
   *
   * @param spark Implicit SparkSession required to perform the transformations.
   * @param path The path to the CSV file.
   * @return DataFrame containing the CSV data.
   */
  def readData(implicit spark: SparkSession , path :String): DataFrame = {
    spark
      .read
      .format("csv")
      .option("header", "true")        // Indicates that the first row contains column names.
      .option("inferSchema", "true")   // Automatically infers the data types of the columns.
      .load(path)                       // Loads the data from the specified path.
  }
}
