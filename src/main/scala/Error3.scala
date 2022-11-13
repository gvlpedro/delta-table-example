import org.apache.spark.sql.SparkSession

/**
 * Trying to create 'delta' table (it should be the degault format)
 * ERROR: Hive support is required to CREATE Hive TABLE (AS SELECT)
 */
object Error3 {
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
         |""".stripMargin

    spark.sql(createTable)
    spark.sql("show create table telemetry")
  }
}