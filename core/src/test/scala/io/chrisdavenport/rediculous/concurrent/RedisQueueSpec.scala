package io.chrisdavenport.rediculous.concurrent

import scala.concurrent.duration._

import cats.effect.IO
import cats.syntax.all._
import fs2.Stream
import fs2.concurrent.Queue
import io.chrisdavenport.rediculous.RedisConnection


trait RedisQueueSpec extends RedisSpec {

  redisResource.test("it should try dequeue1 when the queue is empty") { redis =>
    val queue = redisQueue(redis, Random.key)
    queue.tryDequeue1.map(x =>assert(x.isEmpty))
  }

  redisResource.test("it should try dequeue1 when the queue has elements") { redis =>
    val expected = Random.value
    val queue = redisQueue(redis, Random.key)
    queue.enqueue1(expected) >>
    queue.tryDequeue1.map(assertEquals(_, expected.some))
  }

  redisResource.test("it should enqueue1 and dequeue1") { redis =>
    val expected = Random.value
    val queue = redisQueue(redis, Random.key)
    queue.enqueue1(expected) >>
    queue.dequeue1.map(assertEquals(_, expected))
  }

  def redisQueue(redis: RedisConnection[IO], queueKey: String): Queue[IO, String]

}

class RedisBoundedQueueSpec extends RedisQueueSpec {
  def redisQueue(redis: RedisConnection[IO], queueKey: String): Queue[IO,String] = 
    RedisQueue.boundedQueue(redis, queueKey, 1000, 10.millis)
}

class RedisUnBoundedQueueSpec extends RedisQueueSpec {
  def redisQueue(redis: RedisConnection[IO], queueKey: String): Queue[IO,String] = 
    RedisQueue.unboundedQueue(redis, queueKey, 10.millis)
}

class RedisUnboundedStackSpec extends RedisQueueSpec {
  def redisQueue(redis: RedisConnection[IO], queueKey: String): Queue[IO,String] = 
    RedisQueue.unboundedStack(redis, queueKey, 10.millis)
}

class RedisBoundedStackSpec extends RedisQueueSpec {
  def redisQueue(redis: RedisConnection[IO], queueKey: String): Queue[IO,String] = 
    RedisQueue.boundedStack(redis, queueKey, 10, 10.millis)
}