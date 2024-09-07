package ae.network.migration.spark.DataHandling
import org.apache.spark.sql.{DataFrame, SparkSession}
object DataReader {

  /**
   * @param spark Implicit SparkSession required to perform the transformations.
   * @param path for the CSV file
   * The inferSchema is for setting the data type for the column.
   * @return
   */
  def readingCSV(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .format("csv")
      .option("inferSchema","true")
      .option("header", "true")
      .load(path)

      }
}
