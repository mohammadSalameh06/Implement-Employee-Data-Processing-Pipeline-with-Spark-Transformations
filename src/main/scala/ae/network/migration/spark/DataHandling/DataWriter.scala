package ae.network.migration.spark

import org.apache.spark.sql.{DataFrame, SaveMode}
import com.databricks.spark.xml._

import org.apache.spark.sql.functions._
object DataWriter {

  def writeNormalizedXML(normalizedDF: DataFrame, path: String): Unit = {
    normalizedDF.write
      .mode("overwrite")
      .option("rootTag", "Data")
      .option("rowTag", "Employee")
      .xml("src/main/resources/output/normalized_data.xml")
  }



  def writeNDJSON(employeeDF: DataFrame): Unit = {
    employeeDF.write
      .mode("overwrite")
      .json("src/main/resources/output/ndjson")


  }
  def writePartitionedCSV(df: DataFrame, outputDir: String): Unit = {
    df.write
      .mode("overwrite")
      .partitionBy("department_name", "joining_year")
      .option("header", true)
      .csv(outputDir)
  }
}