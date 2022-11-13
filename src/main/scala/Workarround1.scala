import io.delta.tables.DeltaTable
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 *  Empty table
 */
object Workarround1 {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("delta-test")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
 import spark.implicits._

    val schema = StructType(
      StructField("id",IntegerType, nullable = true) ::
        StructField("value", StringType, nullable = false) :: Nil)

    val localPath="/Users/pedrogarcia/Documents/GITCLONE/delta-test/error4"
    val df:DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    df .write.format("delta").save(localPath)

    val createTable =
      s""" CREATE TABLE IF NOT EXISTS telemetry (
         |  id INT,
         |  value STRING
         |)
         |  USING DELTA
         |  LOCATION '$localPath'
         |""".stripMargin

    spark.sql(createTable)
    spark.sql("select * from telemetry").show(false)

    val df2 = Seq((1, "5"), (2, "6")).toDF("id", "value")
    DeltaTable
      .forName("telemetry")
      .as("target")
      .merge(
        df2.as("source"),
        "target.id=source.id"
      )
      .whenMatched()
      .updateAll()
      .whenNotMatched()
      .insertAll()
      .execute()
    spark.sql("select * from telemetry").show(false)
  }

}