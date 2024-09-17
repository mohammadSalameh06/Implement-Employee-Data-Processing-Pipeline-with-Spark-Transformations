package ae.network.migration.spark

import org.apache.spark.sql.SparkSession
import ae.network.migration.spark.DataHandling._
import ae.network.migration.spark.Transformation.DataTransform
import ae.network.migration.spark.config.sparkconfig
/**
 *The 'EmployeeDataProcessing' object is responsible for reading-processing-writing ,
 *  first it reads the data from CSV files ,
 *  then it performs a data transformation ,
 *  and at last writing the codes into XML and ndJson
 */
object EmployeeDataProcessing {
  def main(args: Array[String]): Unit = {

    // Here is the creating of the spark session
   val spark = sparkconfig.sparkBuilder()

    /**
     * Reading the CSV file from there Folders
     */
    val employeeDF = DataReader.readData(spark,"src/test/scala/ae/network/migration/test/testData/Data/EmployeeData/Employee.csv")
    val departmentDF = DataReader.readData(spark,"src/test/scala/ae/network/migration/test/testData/Data/DepartmentData/Department.csv")
    val buildingDF = DataReader.readData(spark,"src/test/scala/ae/network/migration/test/testData/Data/BuildingData/Building.csv")
    val newEmpDF = DataReader.readData(spark,"src/test/scala/ae/network/migration/test/testData/Data/NewEmp/UpdatedEmployeeData.csv")
    val managerDF = DataReader.readData(spark,"src/test/scala/ae/network/migration/test/testData/Data/ManagerData/manager.csv")
    /**
     *DataTransformers from "DataTransform" class
     */
    val normalizedDF = DataTransform.transformEmployeeData(employeeDF,departmentDF,buildingDF)(spark)
    normalizedDF.show()
    val finalemployee = DataTransform.mergeUpdatedEmployeeData(normalizedDF, newEmpDF,managerDF)(spark)
    val dfWithYear = DataTransform.transformAndExtractYear(finalemployee)

    /**
     *Data Writer from "DataWriter" class
     * Dat
     */
    DataWriter.writeNormalizedXML(finalemployee,"src/main/Outputs/normalized_data.xml")
    DataWriter.writeNDJSON(finalemployee,"src/main/Outputs/ndjson")
    DataWriter.writePartitionedCSV(dfWithYear,"src/main/Outputs/PartitionedCSV")


    spark.stop()
  }
}

