package com.roy

import org.apache.spark.sql.{Dataset, SparkSession}

package object xform {

  implicit class AOps(as: Dataset[A]) {
    def -(otherAs: Dataset[A])(implicit spark: SparkSession): Dataset[A] = {
      import spark.implicits._

      as
        .joinWith(otherAs, as("id") === otherAs("id"), "left_outer")
        .map { case (l, r) => (l, Option(r).isEmpty) }
        .filter(_._2)
        .map(_._1)
    }

    //lets pretend codes seq is quite big
    def codeNotInBig(cs: Seq[String])(implicit spark: SparkSession): Dataset[A] = {
      import spark.implicits._
      val cds: Dataset[String] = cs.toDS

      as.joinWith(cds, as("code") === cds("value"), "left_outer")
        .map { case (l, r) => (l, Option(r).isEmpty) }
        .filter(_._2)
        .map(_._1)
    }

    //lets pretend codes seq is quite small
    def codeNotInSmall(cs: Seq[String]): Dataset[A] = {
      val css = cs.toSet

      as.filter(a => !css.contains(a.code))
    }
  }

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

      ss
        .sort(ss("id"), ss("t"))
        .groupByKey(s => s.id)
        .flatMapGroups { (_, ss) =>
          new Iterator[E] {
            var nextStart: Option[S] = None

            override def hasNext: Boolean = ss.hasNext || nextStart.isDefined

            override def next(): E = {
              if (ss.hasNext) {
                val start = nextStart.getOrElse(ss.next())
                var last = start

                while (last.state == start.state && ss.hasNext)
                  last = ss.next()

                if (last.state == start.state) {
                  nextStart = None
                  E(start.id, start.state, start.t, None)
                } else {
                  nextStart = Some(last)
                  E(start.id, start.state, start.t, Some(last.t))
                }
              } else {
                val Some(start) = nextStart
                nextStart = None
                E(start.id, start.state, start.t, None)
              }
            }
          }
        }
    }

    def asOfJoin(rs: Dataset[R])(implicit spark: SparkSession): Dataset[(S, Option[R])] = {
      import spark.implicits._

      ss
        .joinWith(
          rs.sort(rs("fk"), rs("t")),
          ss("id") === rs("fk"),
          "left_outer")
        .map { case (l, r) => (l, Option(r)) }
        .groupByKey { case (s, _) => s }
        .flatMapGroups { (k, vs) =>
          new Iterator[(S, Option[R])] {
            private var didNotStart: Boolean = true

            override def hasNext: Boolean = didNotStart

            override def next(): (S, Option[R]) = {
              didNotStart = false
              vs
                .find { case (l, rOpt) =>
                  rOpt match {
                    case Some(r) => l.t >= r.t
                    case _ => false
                  }
                }.getOrElse((k, None))
            }
          }
        }
    }
  }

}
