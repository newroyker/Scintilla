package com.roy

import org.apache.spark.sql.{Dataset, SparkSession}

package object xform {

  implicit class EOps(es: Dataset[E]) {

    def counts(implicit spark: SparkSession): Dataset[EC] = {
      import spark.implicits._

      val bs: Dataset[B] = es
        .flatMap(e => Seq(B(e.s, "start"), B(e.e.get, "end")))

      val ts: Dataset[Int] = bs
        .map(b => b.t)
        .distinct()

      val bbs: Dataset[(Int, B)] = ts
        .joinWith(
          bs,
          ts("value") >= bs("t"),
          "left_outer")

      bbs
        .groupByKey { case (l, _) => l }
        .mapGroups { case (k, vs) =>
          val count: Int = vs
            .map { case (_, b) => b.bt }
            .foldLeft(0) { (c, v) =>
              v match {
                case "start" => c + 1
                case "end" => c - 1
              }
            }
          EC(k, count)
        }
    }
  }

  implicit class SOps(ss: Dataset[S]) {

    def toEs(implicit spark: SparkSession): Dataset[E] = {
      import spark.implicits._

      def getNext(sds: Dataset[S]): Dataset[(Int, Option[S])] = {
        val ts: Dataset[Int] = sds
          .map(_.t)

        ts
          .joinWith(
            sds,
            ts("value") < sds("t"),
            "left_outer")
          .map { case (l, r) => (l, Option(r)) }
          .groupByKey { case (k, _) => k }
          .reduceGroups { (x, y) =>
            (x, y) match {
              case ((_, Some(sx)), (_, Some(sy))) => if (sx.t < sy.t) x else y
              case _ => y
            }
          }
          .map { case (_, r) => r }
      }

      val next: Dataset[(Int, Option[S])] = getNext(ss)

      val redundant: Dataset[S] = ss
        .joinWith(
          next,
          next("_1") === ss("t"))
        .filter { r =>
          r match {
            case (a, (_, Some(b))) => a.id == b.id
            case _ => false
          }
        }
        .map { case (_, (_, Some(s))) => s }

      val cleanSS: Dataset[S] = ss
        .except(redundant)

      val newNext: Dataset[(Int, Option[S])] = getNext(cleanSS)

      cleanSS
        .joinWith(
          newNext,
          newNext("_1") === cleanSS("t"))
        .map { case (a, (_, b)) => E(a.id, a.t, b.map(_.t)) }
    }

    def asOfJoin(rs: Dataset[R])(implicit spark: SparkSession): Dataset[(S, Option[R])] = {
      import spark.implicits._

      ss
        .joinWith(
          rs,
          ss("id") === rs("fk") && ss("t") >= rs("t"),
          "left_outer")
        .map { case (l, r) => (l, Option(r)) }
        .groupByKey { case (s, _) => s }
        .reduceGroups { (x, y) =>
          (x, y) match {
            case ((_, Some(R(tx, _))), (_, Some(R(ty, _)))) => if (tx > ty) x else y
            case _ => x
          }
        }
        .map { case (_, r) => r }
    }
  }

}
