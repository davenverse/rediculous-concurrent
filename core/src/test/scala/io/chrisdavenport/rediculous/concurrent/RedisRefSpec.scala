package io.chrisdavenport.rediculous.concurrent

import scala.concurrent.duration._

import cats.effect._
import cats.effect.concurrent._
import cats.syntax.all._
import io.chrisdavenport.rediculous.RedisCommands
import io.chrisdavenport.rediculous.RedisConnection

trait RedisRefSpec extends RedisSpec {

  redisResource.test("it should get the init state") { redis =>
    val expectedValue = Random.value
    redisRef(redis, Random.key, expectedValue) >>= { ref =>
      ref.get.map(assertEquals(_, expectedValue))
    } 
  }
  redisResource.test("it should set and get the value") { redis =>
    val expectedValue = Random.value
    redisRef(redis, Random.key, expectedValue) >>= { ref =>
      ref.set(expectedValue) >>
      ref.get.map(assertEquals(_, expectedValue))
    }
  }

  redisResource.test("it should update the value") { redis =>
    val initValue = Random.value
    redisRef(redis, Random.key, initValue) >>= { ref =>
      ref.update(_ ++ "!") >>
      ref.get.map(assertEquals(_, s"${initValue}!"))
    }
  }

  redisResource.test("it should modify the value") { redis =>
    val initValue = Random.value
    redisRef(redis, Random.key, initValue) >>= { ref =>
      ref.modify(x => x ++ "!"-> x) >>= { out =>
        assertEquals(out, initValue) 
        ref.get.map(assertEquals(_, s"${initValue}!"))
      }
    }
  }

  redisResource.test("it should access") { redis => 
    val initValue = Random.value
    redisRef(redis, Random.key, initValue) >>= { ref =>
      ref.access >>= { case(value, setter) =>
        setter(value + "!") >>
        ref.get.map(assertEquals(_, s"${initValue}!"))
      }
    }
  }

  def redisRef(redis: RedisConnection[IO], key: String, setIfAbsent: String): IO[Ref[IO, String]]

}


class WatchApproachRedisRef extends RedisRefSpec {
  def redisRef(redis: RedisConnection[IO], key: String, setIfAbsent: String): IO[Ref[IO,String]] = 
    RedisRef.atLocation(redis, key, setIfAbsent)

}
class LockedApproachRedisRef extends RedisRefSpec {
  def redisRef(redis: RedisConnection[IO], key: String, setIfAbsent: String): IO[Ref[IO,String]] = 
    RedisRef.lockedLocation(redis, key, setIfAbsent, 1.second, 1.second, RedisCommands.SetOpts.default)
}