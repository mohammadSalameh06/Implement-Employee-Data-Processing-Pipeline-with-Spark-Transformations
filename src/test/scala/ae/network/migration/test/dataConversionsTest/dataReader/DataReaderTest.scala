package ae.network.migration.test.DataHandling

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import ae.network.migration.spark.DataHandling.DataReader

class DataReaderTest extends AnyFunSuite {

  val path = "src/test/scala/ae/network/migration/test/testData/Data/EmployeeData/Employee.csv"

  implicit val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("DataReaderTest")
    .getOrCreate()

  test("DataReader should load CSV and contain expected column names") {

    val expectedColumns = Array("id", "email", "name", "department_id", "job_id")
    val employeeDF = DataReader.readData(spark, path)

    assert(employeeDF.columns.sorted sameElements expectedColumns.sorted)
  }

  test("The DataReader will fail when the expeted colum don't match") {
    val employeeDF = DataReader.readData(spark, path)

    val incorrectColumns = Array("id", "username", "first_name", "department", "position")
    assert(!(employeeDF.columns.sorted sameElements incorrectColumns.sorted), "The columns should not match the incorrect expected columns")


  }
}
