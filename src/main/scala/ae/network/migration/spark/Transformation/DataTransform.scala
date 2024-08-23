package ae.network.migration.spark.Transformation

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DataTransform {

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
  }

  def mergeUpdatedEmployeeData(normalizedDF: DataFrame, newEmpDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._


    val managerDF :DataFrame = normalizedDF.select(
      $"employee_id".as("manager_id"),

      $"name".as("manager_name"),

      $"employee_email".as("manager_email")
    )





    val finalEmployeeDF: DataFrame = normalizedDF
      .join(newEmpDF, $"employee_id" === $"id", "left")
      .join(managerDF,normalizedDF("manager_id") === managerDF("manager_id"))
      .select(
        $"employee_id".as("id"),
        normalizedDF("employee_email"),
        $"name",
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
        $"salary",
        $"joining_date",
        $"contract_type"
      )

    finalEmployeeDF
  }



// i want to make a function that prints employee and managers in a seperate  dataframe to check that the data perform in the right way ,

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

