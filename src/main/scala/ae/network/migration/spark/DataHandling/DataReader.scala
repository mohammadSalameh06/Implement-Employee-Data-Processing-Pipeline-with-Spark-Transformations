package ae.network.migration.spark.DataHandling
import org.apache.spark.sql.{DataFrame, SparkSession}
object DataReader {

  /**
   * @param spark Implicit SparkSession required to perform the transformations.
   * @param path for the CSV file
   * The inferSchema is for setting the data type for the column.
   * @return
   */
  def readData(spark: SparkSession): (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {
    val employeesDF = spark.read.option("header", "true").csv("src/test/scala/ae/network/migration/test/testData/Data/EmployeeData/Employee.csv")
    val departmentsDF = spark.read.option("header", "true").csv("src/test/scala/ae/network/migration/test/testData/Data/DepartmentData/Department.csv")
    val buildingsDF = spark.read.option("header", "true").csv("src/test/scala/ae/network/migration/test/testData/Data/BuildingData/Building.csv")
    val newEmpDF = spark.read.option("header", "true").csv("src/test/scala/ae/network/migration/test/testData/Data/NewEmp/UpdatedEmployeeData.csv")
    val managerDF = spark.read.option("header", "true").csv("src/test/scala/ae/network/migration/test/testData/Data/ManagerData/manager.csv")

    (employeesDF, departmentsDF, buildingsDF, newEmpDF, managerDF)
  }
}
