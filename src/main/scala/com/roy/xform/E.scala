package com.roy.xform

import java.util.UUID

case class E(id: String, s: Int, e: Int)

object E{
  def apply(s: Int, e: Int): E = E(UUID.randomUUID.toString, s, e)
}
