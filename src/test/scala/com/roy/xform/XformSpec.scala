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

    val ss: Dataset[S] = Seq(S("a", 1), S("b", 2), S("b", 3), S("a", 4), S("a", 5), S("a", 6), S("c", 9))
      .toDS

    val es: Dataset[E] = ss
      .toEs

    //es.explain()

    es.collect() must contain theSameElementsAs
      Seq(E("a", 1, Some(2)), E("b", 2, Some(4)), E("a", 4, Some(9)), E("c", 9, None))

  }

  it should "enrich Ss with Rs using as of join" in {

    val ss: Dataset[S] = Seq(S("a", 1), S("a", 3), S("b", 5), S("b", 7))
      .toDS

    val rs: Dataset[R] = Seq(R(0, "a"), R(2, "a"), R(6, "b"))
      .toDS

    val srs: Dataset[(S, Option[R])] = ss
      .asOfJoin(rs)

    //srs.explain()

    srs.collect() must contain theSameElementsAs
      Seq((S("a", 1), Some(R(0, "a"))), (S("a", 3), Some(R(2, "a"))), (S("b", 5), None), (S("b", 7), Some(R(6, "b"))))

  }
}
