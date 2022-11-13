import org.apache.spark.sql.SparkSession

/**
 *   Trying to create a simple delta table with saveAsTable.
 *   ERROR: Table implementation does not support writes: default.telemetry
 */
object Error2 {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("delta-test")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    Seq("1","3","4").toDF("value").write.format("delta").saveAsTable("telemetry")
  }
}