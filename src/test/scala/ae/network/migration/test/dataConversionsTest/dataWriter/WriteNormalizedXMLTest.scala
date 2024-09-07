package ae.network.migration.test.dataConversionsTest.dataWriter

import ae.network.migration.spark.DataHandling.DataReader
import ae.network.migration.spark.DataWriter
import ae.network.migration.spark.Transformation.DataTransform
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

import scala.xml.XML

class WriteNormalizedXMLTest extends AnyFunSuite {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("WriteNormalizedXMLTest")
    .master("local")
    .getOrCreate()

  /**
   * Tests the functionality of the `writeNormalizedXML` method in the `DataWriter` class,
   * and this test suites that `writeNormalizedXML` write an xml file with a expected structure ,
   * and that happens after transforming csv files using the the DataTransform function.
   */
  test("writeNormalizedXML should write XML file with correct structure after transformations") {
    val employeeDF = DataReader.readingCSV(spark,"src/test/scala/ae/network/migration/test/testData/Data/EmployeeData/Employee.csv")
    val departmentDF = DataReader.readingCSV(spark,"src/test/scala/ae/network/migration/test/testData/Data/DepartmentData/Department.csv")
    val buildingDF = DataReader.readingCSV(spark,"src/test/scala/ae/network/migration/test/testData/Data/BuildingData/Building.csv")

    val normalizedDF = DataTransform.transformEmployeeData(employeeDF, departmentDF, buildingDF)

    DataWriter.writeNormalizedXML(normalizedDF,"src/test/scala/ae/network/migration/test/outputTest/xml")

    val xml = XML.loadFile("src/test/scala/ae/network/migration/test/outputTest/xml/part-00000")
    assert((xml \\ "Employee").nonEmpty, "XML should contain Employee elements")
    assert((xml \\ "employee_email").nonEmpty, "XML should contain employee_email elements")
    assert((xml \\ "department_name").nonEmpty, "XML should contain department_name elements")
  }
}
