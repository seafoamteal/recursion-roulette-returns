package net.rroulette.flights4j

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object FlightsApp extends App {
  val (inputFile, origin, dest) = (args(0), args(1), args(2))
  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("flight_data")

  Runner.run(conf, inputFile, origin, dest)
}

object Runner {
  def run(conf: SparkConf, inputFile: String, origin: String, dest: String): Unit = {
    val spark = SparkSession.builder()
      .config(conf)
      .config("spark.driver.memory", "8g")
      .config("spark.driver.maxResultSize", "4g")
      .config("spark.default.parallelism", "12")
      .config("spark.sql.shuffle.partitions", "12")
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", "file:///tmp/spark-events")
      .getOrCreate()

    val rdd = spark.read
      .parquet(inputFile)
      .select(
        "Date",
        "AirlineCode",
        "FlightNumber",
        "Origin",
        "Dest",
        "ArrDelay",
        "Cancelled",
        "Diverted"
      )
      .rdd

    val result = rdd
      .filter { r =>
        r.getAs[Double]("Cancelled") != 1.0 &&
        r.getAs[Double]("Diverted") != 1.0 &&
        !r.isNullAt(r.fieldIndex("DepDelay")) &&
        r.getAs[Double]("DepDelay") >= 0.0
      }
      .map { r =>
        (r.getAs[String]("Origin"), r.getAs[Double]("DepDelayMinutes"))
      }
      .aggregateByKey((0.0, 0))(
        { case ((sum, count), delay) => (sum + delay, count + 1) },
        { case ((sum1, count1), (sum2, count2)) =>
          (sum1 + sum2, count1 + count2)
        }
      )
      .map { case (origin, (sum, count)) => (origin, sum / count) }
      .sortBy(_._2)

    result.take(20).foreach(println)
  }
}
