package ae.network.migration.test.transformersTest

import ae.network.migration.spark.DataHandling.DataReader
import ae.network.migration.spark.Transformation.DataTransform

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class TransformEmployeeDataTest extends AnyFunSuite {

  implicit val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("TransformEmployeeDataTest")
    .getOrCreate()

  val employeeDF = DataReader.readData(spark,"src/test/scala/ae/network/migration/test/testData/Data/EmployeeData/Employee.csv")
  val departmentDF = DataReader.readData(spark,"src/test/scala/ae/network/migration/test/testData/Data/DepartmentData/Department.csv")
  val buildingDF = DataReader.readData(spark,"src/test/scala/ae/network/migration/test/testData/Data/BuildingData/Building.csv")

  val resultDF = DataTransform.transformEmployeeData(employeeDF, departmentDF, buildingDF)
  test("transformEmployeeData should correctly join and normalize data") {

    assert(resultDF.columns.contains("employee_id"))
    assert(resultDF.columns.contains("department_name"))
    assert(resultDF.count() > 0)
      }

  test("transformEmployeeData should fail with incorrect DataFrame structure") {


    assert(!resultDF.columns.contains("employee_id"))
    assert(!resultDF.columns.contains("department_name"))
    assert(resultDF.count() == 0)
  }

}