package com.test.sample

import org.apache.spark.sql.SparkSession

/**
 *   Trying to create a simple delta table with SQL.
 *   ERROR: spark-warehouse/telemetry is not a Delta table.
 */
object Error1 {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("delta-test")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val createTable =
      s""" CREATE TABLE IF NOT EXISTS telemetry (
         |  value DOUBLE
         |)
         |  USING DELTA
         |""".stripMargin

    spark.sql(createTable)
    spark.sql("select * from telemetry").show(false)
  }
}