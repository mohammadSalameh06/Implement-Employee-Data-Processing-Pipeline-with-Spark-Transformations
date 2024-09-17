package ae.network.migration.test.transformersTest

import ae.network.migration.spark.DataHandling.DataReader
import ae.network.migration.spark.Transformation.DataTransform
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class MergeUpdatedEmployeeDataTest extends AnyFunSuite {
val path= "src/test/scala/ae/network/migration/test/testData/Data"
  implicit val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("MergeUpdatedEmployeeDataTest")
    .getOrCreate()


  test("mergeUpdatedEmployeeData should merge and enrich  data") {

    val employeeDF = DataReader.readData(spark, s"$path/EmployeeData")
    val departmentDF = DataReader.readData(spark, s"$path/DepartmentData")
    val buildingDF = DataReader.readData(spark, s"$path/BuildingData")
    val newEmpDF = DataReader.readData(spark,s"$path/NewEmp")
    val managerDF = DataReader.readData(spark,s"$path/ManagerData/manager.csv")


    val normalizedDF = DataTransform.transformEmployeeData(employeeDF, departmentDF, buildingDF)

    val resultDF = DataTransform.mergeUpdatedEmployeeData(normalizedDF, newEmpDF,managerDF)

    assert(resultDF.columns.contains("department"))
    assert(resultDF.columns.contains("id"))
    assert(resultDF.columns.contains("city"))
    assert(resultDF.count() > 0)
  }
}
