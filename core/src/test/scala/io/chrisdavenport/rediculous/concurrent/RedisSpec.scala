package io.chrisdavenport.rediculous.concurrent

import munit.CatsEffectSuite

trait RedisSpec extends CatsEffectSuite {
  import resources._
  def redisResource = ResourceFixture(redisConnection)
}