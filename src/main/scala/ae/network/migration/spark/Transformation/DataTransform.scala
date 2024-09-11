package ae.network.migration.spark.Transformation

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DataTransform {
  /**
   * Transforms the employee data by joining it with the department and building data,
   * and normalizes the result into a DataFrame with relevant columns.
   *
   * @param employeesDF  DataFrame containing employee data.
   * @param departmentsDF  DataFrame containing department data.
   * @param buildingDF  DataFrame containing building data.
   * @param spark  Implicit SparkSession required to perform the transformations.
   * @return  A DataFrame containing the normalized employee data, with columns.
   */


  /**
   * name and datatype for the udf , then => for the return type and at last the parameter
   */

  val headmanagers: Int => String = (id: Int) => {
    if (id >= 1 && id <= 10) {
      "Head Manager"
    } else {
      "Employee"
    }
  }

  val headmanagerUDF = udf(headmanagers)

  def transformEmployeeData(employeesDF: DataFrame, departmentsDF: DataFrame, buildingDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val empdepDF: DataFrame = employeesDF.as("e")
      .join(departmentsDF.as("d"), $"e.department_id" === $"d.id")

    val normalizedDF: DataFrame = empdepDF
      .join(buildingDF.as("b"), $"d.building_id" === $"b.id")
      .select(
        employeesDF("id").as("employee_id"),
        employeesDF("email").as("employee_email"),
        employeesDF("name"),
        departmentsDF("department_name"),
        departmentsDF("manager_id"),
        buildingDF("building_number"),
        buildingDF("country"),
        buildingDF("city")
      )



    normalizedDF
.withColumn("manager_status", headmanagerUDF(normalizedDF("employee_id")))
  }

  /**
   * Merges the normalized employee data with new employee data and enriches it by including manager details.
   * @param normalizedDF  DataFrame containing the normalized employee data.
   * @param newEmpDF  DataFrame containing updated employee data.
   *
   */
  def mergeUpdatedEmployeeData(normalizedDF: DataFrame, newEmpDF: DataFrame, managerDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._




    val finalEmployeeDF: DataFrame = newEmpDF
      .join(managerDF, newEmpDF("direct_manager_id") === managerDF("manager_id"))
      .join(normalizedDF, newEmpDF("id") === normalizedDF("employee_id"))

      .select(
        normalizedDF("employee_id").as("id"),
        normalizedDF("employee_email"),
        normalizedDF("name"),

        struct(
          struct(
            managerDF("manager_id"),
            managerDF("manager_name"),
            managerDF("manager_email"),
          ).as("Manager"),
          $"department_name",
          $"building_number"
        ).as("department"),
        $"country",
        $"city",
        newEmpDF("salary"),
        newEmpDF("joining_date"),
        newEmpDF("contract_type")
      )

    finalEmployeeDF
  }
  /**
   *i want to make a function that prints employee and managers in a seperate  dataframe to check that the data perform in the right way ,
   */
  def transformAndExtractYear(df: DataFrame): DataFrame = {
    df.withColumn(
        "joining_date",
        to_date(col("joining_date"), "M/d/yyyy")
      )
      .withColumn(
        "joining_year",
        year(col("joining_date"))).withColumn("department_name", col("department.department_name"))
          .drop("department")

  }
}

