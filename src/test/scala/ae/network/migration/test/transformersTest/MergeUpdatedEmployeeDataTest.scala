package ae.network.migration.test.transformersTest

import ae.network.migration.test.dataConversionsTest.dataReader.DataReader
import ae.network.migration.spark.Transformation.DataTransform
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class MergeUpdatedEmployeeDataTest extends AnyFunSuite {

  implicit val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("MergeUpdatedEmployeeDataTest")
    .getOrCreate()


  test("mergeUpdatedEmployeeData should merge and enrich data") {

    val (employeesDF, departmentsDF, buildingsDF, newEmpDF, managerDF) = DataReader.readData(spark)



    val normalizedDF = DataTransform.transformEmployeeData(employeesDF, departmentsDF, buildingsDF)

    val resultDF = DataTransform.mergeUpdatedEmployeeData(normalizedDF, newEmpDF,managerDF)

    assert(resultDF.columns.contains("department"))
    assert(resultDF.columns.contains("id"))
    assert(resultDF.columns.contains("city"))
    assert(resultDF.count() > 0)
  }
}
