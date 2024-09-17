package ae.network.migration.test.transformersTest

import ae.network.migration.spark.DataHandling.DataReader
import ae.network.migration.spark.Transformation.DataTransform
//import ae.network.migration.test.dataConversionsTest.dataReader.DataReader

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class TransformAndExtractYearTest extends AnyFunSuite {

  implicit val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("TransformAndExtractYearTest")
    .getOrCreate()
  val employeesDF = DataReader.readData(spark,"src/test/scala/ae/network/migration/test/testData/Data/EmployeeData/Employee.csv")
  val departmentsDF = DataReader.readData(spark,"src/test/scala/ae/network/migration/test/testData/Data/DepartmentData/Department.csv")
  val buildingDF = DataReader.readData(spark,"src/test/scala/ae/network/migration/test/testData/Data/BuildingData/Building.csv")

//  test("transformAndExtractYear should extract year from joining_date") {
//    val (employeeDF, departmentDF, buildingDF, newEmpDF, managerDF) = DataReader.readData(spark)
//
//
//
//    val normalizedDF = DataTransform.transformEmployeeData(employeesDF, departmentsDF, buildingDF)
//
//    val resulltDF = DataTransform.mergeUpdatedEmployeeData(normalizedDF, newEmpDF,managerDF)
//    val resultDF = DataTransform.transformAndExtractYear(resulltDF)
//
//    assert(resultDF.columns.contains("joining_year"))
//    assert(resultDF.count() > 0)
//  }
}
