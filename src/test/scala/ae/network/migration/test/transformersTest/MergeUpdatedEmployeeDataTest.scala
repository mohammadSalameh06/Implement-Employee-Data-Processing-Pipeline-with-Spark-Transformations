package ae.network.migration.test.transformersTest

import ae.network.migration.spark.DataHandling.DataReader
import ae.network.migration.spark.Transformation.DataTransform
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class MergeUpdatedEmployeeDataTest extends AnyFunSuite {

  implicit val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("MergeUpdatedEmployeeDataTest")
    .getOrCreate()


  test("mergeUpdatedEmployeeData should merge and enrich  data") {

    val employeeDF = DataReader.readData(spark,"src/test/scala/ae/network/migration/test/testData/Data/EmployeeData/Employee.csv")
    val departmentDF = DataReader.readData(spark,"src/test/scala/ae/network/migration/test/testData/Data/DepartmentData/Department.csv")
    val buildingDF = DataReader.readData(spark,"src/test/scala/ae/network/migration/test/testData/Data/BuildingData/Building.csv")
    val newEmpDF = DataReader.readData(spark,"src/test/scala/ae/network/migration/test/testData/Data/NewEmp/UpdatedEmployeeData.csv")
    val managerDF = DataReader.readData(spark,"src/test/scala/ae/network/migration/test/testData/Data/ManagerData/manager.csv")


    val normalizedDF = DataTransform.transformEmployeeData(employeeDF, departmentDF, buildingDF)

    val resultDF = DataTransform.mergeUpdatedEmployeeData(normalizedDF, newEmpDF,managerDF)

    assert(resultDF.columns.contains("department"))
    assert(resultDF.columns.contains("id"))
    assert(resultDF.columns.contains("city"))
    assert(resultDF.count() > 0)
  }
}
