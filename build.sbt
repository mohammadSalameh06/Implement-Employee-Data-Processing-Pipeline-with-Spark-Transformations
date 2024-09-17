ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"

val sparkVersion = "3.5.0"

lazy val root = (project in file("."))
  .settings(
    name := "StartingWithSpark",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.0"% "provided",
      "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
      "com.databricks" %% "spark-xml" % "0.14.0",
      "org.scalatest" %% "scalatest" % "3.2.17" % "test"

    )

  )
coverageEnabled := true
coverageExcludedFiles := "ae\\.network\\.migration\\.spark\\.EmployeeDataProcessing"
coverageMinimumStmtTotal := 80
coverageMinimumBranchTotal := 80
coverageFailOnMinimum := true




