package ae.network.migration.test.transformersTest

import ae.network.migration.spark.Transformation.DataTransform
import ae.network.migration.test.dataConversionsTest.dataReader.DataReader

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class TransformEmployeeDataTest extends AnyFunSuite {

  implicit val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("TransformEmployeeDataTest")
    .getOrCreate()


  test("transformEmployeeData should correctly join and normalize data") {
    val (employeesDF, departmentsDF, buildingDF, newEmpDF, managerDF) = DataReader.readData(spark)


    val resultDF = DataTransform.transformEmployeeData(employeesDF, departmentsDF, buildingDF)

    assert(resultDF.columns.contains("employee_id"))
    assert(resultDF.columns.contains("department_name"))
    assert(resultDF.count() > 0)
  }
}
