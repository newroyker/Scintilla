package com.roy

import com.roy.xform.S
import org.apache.spark.sql.functions._
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

  val ss: Dataset[S] = Seq(S("a", 1), S("b", 2), S("b", 3), S("a", 4), S("a", 5), S("c", 7)).toDS

  ss.show()

  ss.toEs
    .orderBy(asc("s"))
    .show

  spark.stop()
}
