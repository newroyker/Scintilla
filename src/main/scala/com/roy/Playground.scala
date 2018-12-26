package com.roy

import com.roy.xform.E
import org.apache.spark.sql.SparkSession

object Playground extends App {
  val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("PlaygroundApp")
    .config("spark.driver.host", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val es = Seq(E(1, 5), E(2, 6), E(3, 7), E(4, 8), E(5, 9)).toDS

  es.show()

  spark.stop()
}
