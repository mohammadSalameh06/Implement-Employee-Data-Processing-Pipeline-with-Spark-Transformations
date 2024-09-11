package ae.network.migration.spark

import org.apache.spark.sql.SparkSession

import ae.network.migration.spark.DataHandling._
import ae.network.migration.spark.Transformation.DataTransform
/**
 *The 'EmployeeDataProcessing' object is responsible for reading-processing-writing ,
 *  first it reads the data from CSV files ,
 *  then it performs a data transformation ,
 *  and at last writing the codes into XML and ndJson
 */
object EmployeeDataProcessing {
  def main(args: Array[String]): Unit = {

    // Here is the creating of the spark session
    val spark = SparkSession.builder()
      .appName("StartingWithSpark")
      .master("local[*]")
      .getOrCreate()

    /**
     * Reading the CSV file from there Folders
     */
    val (employeesDF, departmentsDF, buildingDF, newEmpDF, managerDF) = DataReader.readData(spark)

    /**
     *DataTransformers from "DataTransform" class
     */
    val normalizedDF = DataTransform.transformEmployeeData(employeesDF,departmentsDF,buildingDF)(spark)
    normalizedDF.show()
    val finalemployee = DataTransform.mergeUpdatedEmployeeData(normalizedDF, newEmpDF,managerDF)(spark)
    val dfWithYear = DataTransform.transformAndExtractYear(finalemployee)

    /**
     *Data Writer from "DataWriter" class
     */
    DataWriter.writeNormalizedXML(finalemployee,"src/main/Outputs/normalized_data.xml")
    DataWriter.writeNDJSON(finalemployee,"src/main/Outputs/ndjson")
    DataWriter.writePartitionedCSV(dfWithYear,"src/main/Outputs/PartitionedCSV")


    spark.stop()
  }
}

