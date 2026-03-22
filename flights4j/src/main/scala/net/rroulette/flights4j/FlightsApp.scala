package net.rroulette.flights4j

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import java.io.FileWriter

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

    val base = spark.read
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

      .map { row =>
        (
          row.getAs[String]("AirlineCode") + row.getAs[String]("FlightNumber"),
          row.getAs[java.sql.Date]("Date").toString(),
          row.getAs[String]("Origin"),
          row.getAs[String]("Dest"),
          row.getAs[Double]("ArrDelay"),
          row.getAs[Double]("Cancelled"),
          row.getAs[Double]("Diverted"),
          row.isNullAt(row.fieldIndex("ArrDelay")),
          row.getAs[String]("Origin") == origin && row.getAs[String]("Dest") == dest
        )
      }

     val activeRoutes = base
      .map { 
        case (flightCode, flightDate, _, _, _, _, _, _, isRequestedRoute) => (flightCode, (flightDate, isRequestedRoute))
      }
      .reduceByKey { 
        (a, b) => if (a._1 > b._1) a else b
      }
      .filter { 
        case (_, (flightDate, flightMatchesRoute)) => flightMatchesRoute && flightDate >= "2025-01-01"
      }
      .map { case (flightCode, _) =>
        (flightCode, ())
      }

      val result = base
        .filter {
          case (_, _, rowOrigin, rowDest, _, cancelled, diverted, arrDelayIsNull, _) =>
          cancelled != 1.0 &&
          diverted != 1.0 &&
          !arrDelayIsNull &&
          rowOrigin == origin && rowDest == dest
        }
        .map { 
        case (flightCode, _, _, _, arrivalDelay, _, _, _, _) => (flightCode, arrivalDelay)
        }
        .join(activeRoutes)
        .mapValues { 
          case (arrivalDelay, _) =>(arrivalDelay, 1)
        }
        .reduceByKey { 
          (a, b) => (a._1 + b._1, a._2 + b._2)
        }
        .filter { 
          case (_, (_, flightCount)) => flightCount >= 10
        }
        .map { 
          case (flightCode, (totalDelay, flightCount)) => (flightCode, totalDelay / flightCount, flightCount)
        }
        .sortBy { 
          case (_, averageDelay, _) => averageDelay
        }

        val result = result.take(20)

        val fw = new FileWriter(s"sparkData/${origin}_${dest}_result.csv")
        try {
          fw.write("FlightCode,AverageDelay,FlightCount\n")
          result.foreach {
            case (flightCode, averageDelay, flightCount) => fw.write(s"$flightCode,$averageDelay,$flightCount\n")
          }
        } finally {
          fw.close()
        }

  }
}
