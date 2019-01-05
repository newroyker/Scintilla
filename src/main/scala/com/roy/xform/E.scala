package com.roy.xform

import java.util.UUID

// event where start (s) is inclusive, end (e) is exclusive
case class E(id: Int, state: String, s: Int, e: Option[Int])

object E {
  def apply(s: Int, e: Int): E = E(util.Random.nextInt(), UUID.randomUUID.toString, s, Some(e))
}

//count of events at t
case class EC(t: Int, count: Int)

//time bound of type start or end
case class B(t: Int, bt: String)

//snapshot with id at t
case class S(id: Int, state: String, t: Int)

//reference data at t
case class R(t: Int, fk: Int)
