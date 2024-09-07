package ae.network.migration.test.dataConversionsTest.dataWriter

import ae.network.migration.spark.DataHandling.DataReader
import ae.network.migration.spark.DataWriter
import ae.network.migration.spark.Transformation.DataTransform
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

import scala.io.Source

class WriteNDJSONTest extends AnyFunSuite {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("WriteNDJSONTest")
    .master("local")
    .getOrCreate()

  /**
   * Tests the functionality of the `NDJSONTest` method in the `DataWriter` class,
   * and this test suites that `NDJSONTest` write an xml file with a expected structure ,
   * and that happens after transforming csv files using the the DataTransform function.
   */

  test("writeNDJSON should write NDJSON file with correct data") {
    val employeeDF =
      DataReader.readingCSV(spark,"src/test/scala/ae/network/migration/test/testData/Data/EmployeeData/Employee.csv")
    val departmentDF = DataReader.readingCSV(spark,"src/test/scala/ae/network/migration/test/testData/Data/DepartmentData/Department.csv")
    val buildingDF = DataReader.readingCSV(spark,"src/test/scala/ae/network/migration/test/testData/Data/BuildingData/Building.csv")



    val normalizedDF = DataTransform.transformEmployeeData(employeeDF, departmentDF, buildingDF)

    DataWriter.writeNDJSON(normalizedDF,"src/test/scala/ae/network/migration/test/outputTest/ndJson")


  }
}
