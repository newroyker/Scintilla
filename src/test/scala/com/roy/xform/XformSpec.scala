package com.roy.xform

import com.roy.SparkSupport
import org.apache.spark.sql.Dataset
import org.scalatest.{FlatSpec, MustMatchers}

class XformSpec extends FlatSpec with MustMatchers with SparkSupport {

  import spark.implicits._

  "EOps" should "return counts at time t given events" in {

    val es: Dataset[E] = Seq(E(1, 3), E(2, 4), E(3, 5))
      .toDS

    val cs: Dataset[EC] = es
      .counts

    //cs.explain()

    cs.collect() must contain theSameElementsAs
      Seq(EC(1, 1), EC(2, 2), EC(3, 2), EC(4, 1), EC(5, 0))
  }

  "SOps" should "return Es" in {

    val ss: Dataset[S] = Seq(S(100, "a", 1), S(100, "b", 2), S(100, "b", 3), S(100, "a", 4), S(100, "a", 5), S(100, "a", 6), S(100, "c", 9), S(100, "c", 11))
      .toDS

    val es: Dataset[E] = ss
      .toEs

    //es.explain()

    es.collect() must contain theSameElementsAs
      Seq(E(100, "a", 1, Some(2)), E(100, "b", 2, Some(4)), E(100, "a", 4, Some(9)), E(100, "c", 9, None))

  }

  it should "return Es even when they are not sorted" in {

    val ss: Dataset[S] = Seq(S(100, "a", 1), S(100, "b", 2), S(100, "b", 3), S(100, "a", 4), S(100, "a", 5), S(100, "a", 6), S(100, "c", 9), S(100, "c", 11))
      .reverse
      .toDS

    val es: Dataset[E] = ss
      .toEs

    //es.explain()

    es.collect() must contain theSameElementsAs
      Seq(E(100, "a", 1, Some(2)), E(100, "b", 2, Some(4)), E(100, "a", 4, Some(9)), E(100, "c", 9, None))

  }

  it should "enrich Ss with Rs using as of join" in {

    val ss: Dataset[S] = Seq(S(100, "a", 1), S(100, "a", 3), S(200, "b", 5), S(200, "b", 7))
      .toDS

    val rs: Dataset[R] = Seq(R(0, 100), R(2, 100), R(6, 200))
      .toDS

    val srs: Dataset[(S, Option[R])] = ss
      .asOfJoin(rs)

    //srs.explain()

    srs.collect() must contain theSameElementsAs
      Seq((S(100, "a", 1), Some(R(0, 100))), (S(100, "a", 3), Some(R(2, 100))), (S(200, "b", 5), None), (S(200, "b", 7), Some(R(6, 200))))

  }
}
