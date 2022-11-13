import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession

/**
 * Trying to create a simple delta table with 'Temporary views'.
 * ERROR: Temporary views are not delta tables: `telemetry` is not a Delta table.
 */
object Error4 {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("delta-test")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    Seq((1,"1"), (2,"2"), (3,"3")).toDF("id","value").createTempView("telemetry") // just I want a simple table to check my merges :(

    val df = Seq((1, "5"), (2, "6")).toDF("id", "value")

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