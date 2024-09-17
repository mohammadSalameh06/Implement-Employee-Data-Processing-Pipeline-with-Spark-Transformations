package ae.network.migration.spark.config

import org.apache.spark.sql.SparkSession

object sparkconfig {
  def sparkBuilder ():SparkSession = {

    val spark = SparkSession.builder()
      .appName("SparkCourse")
      .master("local[*]")
      .getOrCreate()
    spark
  }

}
