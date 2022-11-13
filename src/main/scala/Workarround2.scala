import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession

/**
 *  Data with initial data
 */
object Workarround2 {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("delta-test")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val localPath="/Users/pedrogarcia/Documents/GITCLONE/delta-test/workarround"

    import spark.implicits._
    Seq((1,"1"), (2,"2"), (3,"3")).toDF("id","value").write.format("delta").save(localPath)

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

    val df=Seq((1,"5"), (2,"6")).toDF("id","value")

    DeltaTable
      .forName("telemetry")
      .as("target")
      .merge(
        df.as("source"),
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