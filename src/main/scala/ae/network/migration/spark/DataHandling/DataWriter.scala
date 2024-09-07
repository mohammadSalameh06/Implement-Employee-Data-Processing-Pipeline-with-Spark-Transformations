package ae.network.migration.spark

import org.apache.spark.sql.{DataFrame, SaveMode}
import com.databricks.spark.xml._

object DataWriter {

  /**
   * @param normalizedDF the dataframe.
   * @param path the path or location where the file will be saved ot stored.
   * converting the dataframe to xml files.
   */
  def writeNormalizedXML(normalizedDF: DataFrame,path:String): Unit = {
    normalizedDF.write
      .mode("overwrite")
      .option("rootTag", "Data")
      .option("rowTag", "Employee")
      .xml(path)
  }
  /**
   * @param employeeDF
   * converting the dataframe to ndJson file .
   */
  def writeNDJSON(employeeDF: DataFrame, path :String): Unit = {
    employeeDF.write
      .mode("overwrite")
      .json(path)
  }
  def writePartitionedCSV(df: DataFrame, path:String): Unit = {
    df.write
      .mode("overwrite")
      .partitionBy("department_name", "joining_year")
      .option("header", true)
      .csv(path)
  }
}