package ae.network.migration.spark.DataHandling

import org.apache.spark.sql.{DataFrame, SparkSession}


object DataReader {


  def read(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .format("csv")
      .option("inferSchema","true")
      .option("header", "true")
      .load(path)
  }





}
