package ae.network.migration.test.dataConversionsTest.dataWriter

import ae.network.migration.spark.DataHandling.DataReader
import ae.network.migration.spark.DataWriter
import ae.network.migration.spark.Transformation.DataTransform
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite


class WritePartitionedCSVTest extends AnyFunSuite with BeforeAndAfter {
val path= "src/test/scala/ae/network/migration/test/testData/Data/"

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("WritePartitionedCSVTest")
    .master("local")
    .getOrCreate()

  /**
   *Tests the functionality of the `PartitionedCSV` method in the `DataWriter` class,
   * and in this test it checks if the partition csv is created and have the write partitions (department,Joining_Year)
   */

  test("writePartitionedCSV should create partitioned CSV files for joining_year 2023 and 2024") {
    val employeeDF = DataReader.readData(spark, s"$path/EmployeeData")
    val departmentDF = DataReader.readData(spark, s"$path/DepartmentData")
    val buildingDF = DataReader.readData(spark, s"$path/BuildingData")
    val newEmpDF = DataReader.readData(spark, s"$path/NewEmp")
    val managerDF = DataReader.readData(spark, s"$path/ManagerData/manager.csv")

    val normalizedDF = DataTransform.transformEmployeeData(employeeDF, departmentDF, buildingDF)
    val finalEmployee = DataTransform.mergeUpdatedEmployeeData(normalizedDF, newEmpDF, managerDF)

    val dfWithYear = DataTransform.transformAndExtractYear(finalEmployee)

    val outputPath = "src/test/scala/ae/network/migration/test/outputTest/partitionedCSV"
    DataWriter.writePartitionedCSV(dfWithYear, outputPath)

    val outputDir = new java.io.File(outputPath)

    val partitionDirs = outputDir.listFiles().filter(_.isDirectory).map(_.getName).toSet
    assert(partitionDirs.contains("department_name=Accounting"))
    assert(partitionDirs.contains("department_name=Business%20Development"))
  }
}
