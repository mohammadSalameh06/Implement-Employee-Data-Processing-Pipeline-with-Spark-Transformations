package ae.network.migration.test.dataConversionsTest.dataWriter

import ae.network.migration.spark.DataWriter
import ae.network.migration.spark.DataHandling.DataReader
import ae.network.migration.spark.Transformation.DataTransform
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite


class WritePartitionedCSVTest extends AnyFunSuite {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("WritePartitionedCSVTest")
    .master("local")
    .getOrCreate()

  /**
   *Tests the functionality of the `PartitionedCSV` method in the `DataWriter` class,
   * and in this test it checks if the partition csv is created and have the write partitions (department,Joining_Year)
   */

  test("writePartitionedCSV should write partitioned CSV files with correct data") {
    val employeeDF = DataReader.readingCSV(spark,"src/test/scala/ae/network/migration/test/testData/Data/EmployeeData/Employee.csv")
    val departmentDF = DataReader.readingCSV(spark,"src/test/scala/ae/network/migration/test/testData/Data/DepartmentData/Department.csv")
    val buildingDF = DataReader.readingCSV(spark,"src/test/scala/ae/network/migration/test/testData/Data/BuildingData/Building.csv")
    val newEmpDF = DataReader.readingCSV(spark,"src/test/scala/ae/network/migration/test/testData/Data/NewEmp/UpdatedEmployeeData.csv")
    val managerDF = DataReader.readingCSV(spark,"src/test/scala/ae/network/migration/test/testData/Data/ManagerData/manager.csv")

    val normalizedDF = DataTransform.transformEmployeeData(employeeDF, departmentDF, buildingDF)
    val finalemployee = DataTransform.mergeUpdatedEmployeeData(normalizedDF, newEmpDF, managerDF)

    val dfWithYear = DataTransform.transformAndExtractYear(finalemployee)

    DataWriter.writePartitionedCSV(dfWithYear,"src/test/scala/ae/network/migration/test/outputTest/partitionedCSV")


  }
}
