package io.chrisdavenport.rediculous.concurrent

import munit.CatsEffectSuite
import cats.effect._
import cats.syntax.all._
import cats.effect.concurrent._
import io.chrisdavenport.rediculous.RedisConnection
import scala.concurrent.duration._
import io.chrisdavenport.rediculous.RedisCommands
import scala.util.Random


trait RedisRefSpec extends RedisSpec {
  private val random = new Random()

  redisResource.test("it should get the init state") { redis =>
    val expectedValue = randomValue
    redisRef(redis, randomKey, expectedValue) >>= { ref =>
      ref.get.map(assertEquals(_, expectedValue))
    } 
  }
  redisResource.test("it should set and get the value") { redis =>
    val expectedValue = randomValue
    redisRef(redis, randomKey, expectedValue) >>= { ref =>
      ref.set(expectedValue) >>
      ref.get.map(assertEquals(_, expectedValue))
    }
  }

  redisResource.test("it should update the value") { redis =>
    val initValue = randomValue
    redisRef(redis, randomKey, initValue) >>= { ref =>
      ref.update(_ ++ "!") >>
      ref.get.map(assertEquals(_, s"${initValue}!"))
    }
  }

  redisResource.test("it should modify the value") { redis =>
    val initValue = randomValue
    redisRef(redis, randomKey, initValue) >>= { ref =>
      ref.modify(x => x ++ "!"-> x) >>= { out =>
        assertEquals(out, initValue) 
        ref.get.map(assertEquals(_, s"${initValue}!"))
      }
    }
  }

  redisResource.test("it should access") { redis => 
    val initValue = randomValue
    redisRef(redis, randomKey, initValue) >>= { ref =>
      ref.access >>= { case(value, setter) =>
        setter(value + "!") >>
        ref.get.map(assertEquals(_, s"${initValue}!"))
      }
    }
  }
  

  def redisRef(redis: RedisConnection[IO], key: String, setIfAbsent: String): IO[Ref[IO, String]]

  private def randomKey : String = s"key_${random.alphanumeric.take(20).mkString}"
  private def randomValue : String = random.alphanumeric.take(200).mkString
}


class WatchApproachRedisRef extends RedisRefSpec {
  def redisRef(redis: RedisConnection[IO], key: String, setIfAbsent: String): IO[Ref[IO,String]] = 
    RedisRef.atLocation(redis, key, setIfAbsent)

}
class LockedApproachRedisRef extends RedisRefSpec {
  def redisRef(redis: RedisConnection[IO], key: String, setIfAbsent: String): IO[Ref[IO,String]] = 
    RedisRef.lockedLocation(redis, key, setIfAbsent, 1.second, 1.second, RedisCommands.SetOpts.default)
}