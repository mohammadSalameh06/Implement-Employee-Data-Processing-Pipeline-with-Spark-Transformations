package ae.network.migration.test.dataConversionsTest.dataReader

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataReader {

  def readData(spark: SparkSession): (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {
    val employeesDF = spark.read.option("header", "true").csv("src/test/scala/ae/network/migration/test/testData/Data/EmployeeData/Employee.csv")
    val departmentsDF = spark.read.option("header", "true").csv("src/test/scala/ae/network/migration/test/testData/Data/DepartmentData/Department.csv")
    val buildingsDF = spark.read.option("header", "true").csv("src/test/scala/ae/network/migration/test/testData/Data/BuildingData/Building.csv")
    val newEmpDF = spark.read.option("header", "true").csv("src/test/scala/ae/network/migration/test/testData/Data/NewEmp/UpdatedEmployeeData.csv")
    val managerDF = spark.read.option("header", "true").csv("src/test/scala/ae/network/migration/test/testData/Data/ManagerData/manager.csv")

    (employeesDF, departmentsDF, buildingsDF, newEmpDF, managerDF)
  }
}
