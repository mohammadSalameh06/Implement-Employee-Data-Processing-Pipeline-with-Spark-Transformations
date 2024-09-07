package ae.network.migration.test.DataHandling

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import ae.network.migration.spark.DataHandling.DataReader

class DataReaderTest extends AnyFunSuite {

  implicit val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("DataReaderTest")
    .getOrCreate()

  test("DataReader should load CSV and contain expected column names") {
    val employeeDF = DataReader.readingCSV(spark, "src/test/scala/ae/network/migration/test/testData/Data/EmployeeData/Employee.csv")

    val expectedColumns = Array("id", "email", "name", "department_id", "job_id")

    assert(employeeDF.columns.sorted sameElements expectedColumns.sorted)
  }
}
