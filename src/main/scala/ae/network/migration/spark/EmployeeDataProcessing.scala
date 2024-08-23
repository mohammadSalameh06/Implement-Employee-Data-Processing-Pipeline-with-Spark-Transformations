package ae.network.migration.spark

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import ae.network.migration.spark.Models._
import ae.network.migration.spark.DataHandling._
import ae.network.migration.spark.Transformation.DataTransform
import org.apache.spark.sql.functions._
import ae.network.migration.spark.Transformation.DataTransform._

/**
 *The 'EmployeeDataProcessing' object is responsible for reading-processing-writing ,
 *  first it reads the data from CSV files ,
 *  then it performs a data transformation ,
 *  and at last writing the codes into XML and ndJson
 */

object EmployeeDataProcessing {

  //Setting a common path so it become more easy for declaring Paths
  val resourcesPath: String = "src/main/scala/ae/network/migration/spark/Resorces/"
  def main(args: Array[String]): Unit = {

    // Here is the creating of the spark session
    val spark = SparkSession.builder()
      .appName("StartingWithSpark")
      .master("local[*]")
      .getOrCreate()

    // Reading input Files (CSV)
    val employeesDF = DataReader.read(spark,s"${resourcesPath}Employee.csv")
    val departmentsDF = DataReader.read(spark,s"${resourcesPath}Department.csv")
    val buildingDF = DataReader.read(spark,s"${resourcesPath}Building.csv")
    val newEmpDF = DataReader.read(spark,s"${resourcesPath}UpdatedEmployeeData.csv")

    // Here is the data processing and the functionality for the data processing is from > Transformation/DataTransform.scala
    val normalizedDF = DataTransform.transformEmployeeData(employeesDF,departmentsDF,buildingDF)(spark)
    val finalemployee = DataTransform.mergeUpdatedEmployeeData(normalizedDF, newEmpDF)(spark)
    val dfWithYear = DataTransform.transformAndExtractYear(finalemployee)

//    val manager :DataFrame = DataTransform.mergeUpdatedEmployeeData()

    finalemployee.show()
    DataWriter.writeNormalizedXML(finalemployee,"src/main/resources/output/normalized_data.xml")
    DataWriter.writeNDJSON(finalemployee)
    DataWriter.writePartitionedCSV(dfWithYear,"src/main/resources/output/PartitionedCSV")


    spark.stop()
  }
}

