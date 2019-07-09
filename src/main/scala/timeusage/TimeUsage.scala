package timeusage

import java.nio.file.Paths

import org.apache.spark.TaskContext
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._

/** Main class */
object TimeUsage {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Time Usage")
      .config("spark.master", "local[4]")
      .getOrCreate()
  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  /** Main function */
  def main(args: Array[String]): Unit = {
    val w = Window.partitionBy($"id", $"warehouse", $"product").orderBy($"wtimestamp".desc)
    val positions = spark.read.csv(fsPath("/timeusage/positions.csv")).toDF("id", "warehouse", "product", "wtimestamp")
    val amounts = spark.read.csv(fsPath("/timeusage/amounts.csv")).toDF("id", "amount", "atimestamp")
    val frame = positions
      .join(amounts, "id")
      .cache()

    frame
      .withColumn("current_amount", first("amount").over(w))
      .groupBy($"id", $"warehouse", $"product")
      .agg(first($"current_amount"))
      .show(false)

    frame
      .groupBy($"warehouse", $"product")
      .agg(min($"amount"), max($"amount"), avg($"amount"))
      .show(false)
  }


  /** @return The filesystem path of the given resource */
  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString
}
