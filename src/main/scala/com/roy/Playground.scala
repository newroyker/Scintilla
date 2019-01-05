package com.roy

import com.roy.xform.{R, S}
import org.apache.spark.sql.{Dataset, SparkSession}

object Playground extends App {
  implicit val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("PlaygroundApp")
    .config("spark.driver.host", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val ss: Dataset[S] = Seq(S(100, "a", 1), S(100, "a", 3), S(200, "b", 5), S(200, "b", 7))
    .toDS

  val rs: Dataset[R] = Seq(R(0, 100), R(2, 100), R(6, 200))
    .toDS

  ss.asOfJoin(rs).show()

  spark.stop()
}
