package io.chrisdavenport.rediculous.concurrent

import scala.concurrent.duration._

import cats.effect.IO
import cats.syntax.all._
import fs2.Stream
import cats.effect.std.Queue
import io.chrisdavenport.rediculous.RedisConnection


trait RedisQueueSpec extends RedisSpec {

  redisResource.test("it should try dequeue1 when the queue is empty") { redis =>
    val queue = redisQueue(redis, Random.key)
    queue.tryTake.map(x =>assert(x.isEmpty))
  }

  redisResource.test("it should try dequeue1 when the queue has elements") { redis =>
    val expected = Random.value
    val queue = redisQueue(redis, Random.key)
    queue.offer(expected) >>
    queue.tryTake.map(assertEquals(_, expected.some))
  }

  redisResource.test("it should enqueue1 and dequeue1") { redis =>
    val expected = Random.value
    val queue = redisQueue(redis, Random.key)
    queue.offer(expected) >>
    queue.take.map(assertEquals(_, expected))
  }

  // redisResource.test("it should be able to enqueue and dequeue multiple elements concurrently") { redis =>
  //   val queue = redisQueue(redis, Random.key)
  //   val expected = Stream.range(1, 4).map(_.toString())
  //   queue
  //     .dequeue
  //     .take(expected.toList.size)
  //     .concurrently(expected.covary[IO].through(queue.enqueue))
  //     .compile.toList.map(x => assertEquals(x.toSet, expected.toList.toSet))
  // }

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