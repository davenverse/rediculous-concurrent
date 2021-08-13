package io.chrisdavenport.rediculous.concurrent

import io.chrisdavenport.rediculous.RedisConnection
import cats.effect.std.Queue

import cats._
import cats.syntax.all._
import cats.effect._
import fs2.Chunk
import io.chrisdavenport.rediculous.RedisCommands
import scala.concurrent.duration.FiniteDuration
import io.chrisdavenport.rediculous.RedisPipeline

object RedisQueue {

  /** rpush/lpop no size bound */
  def unboundedQueue[F[_]: Async](
    redisConnection: RedisConnection[F],
    queueKey: String,
    pollingInterval: FiniteDuration
  ): Queue[F, String] = new RedisQueueUnboundedImpl[F](redisConnection, queueKey, pollingInterval,
    {s => RedisCommands.rpush(queueKey, List(s)).run(redisConnection)} 
  )

  /** lpush/lpop no size bound */
  def unboundedStack[F[_]: Async](
    redisConnection: RedisConnection[F],
    queueKey: String,
    pollingInterval: FiniteDuration
  ): Queue[F, String] = new RedisQueueUnboundedImpl[F](redisConnection, queueKey, pollingInterval,
    {s => RedisCommands.lpush(queueKey, List(s)).run(redisConnection)} 
  )

  /** rpush/lpop size bound, polls on insert when full, can overfill */
  def boundedQueue[F[_]: Async](
    redisConnection: RedisConnection[F],
    queueKey: String,
    maxSize: Long, 
    pollingInterval: FiniteDuration
  ): Queue[F, String] = new BoundedQueue[F](redisConnection, queueKey, maxSize, pollingInterval, 
    {s => RedisCommands.rpush(queueKey, List(s)).run(redisConnection)}
  )

    /** lpush/lpop size bound, polls on insert when full, can overfill */
  def boundedStack[F[_]: Async](
    redisConnection: RedisConnection[F],
    queueKey: String,
    maxSize: Long, 
    pollingInterval: FiniteDuration
  ): Queue[F, String] = new BoundedQueue[F](redisConnection, queueKey, maxSize, pollingInterval, 
    {s => RedisCommands.lpush(queueKey, List(s)).run(redisConnection)}
  )

  private class RedisQueueUnboundedImpl[F[_]: Async](
    redisConnection: RedisConnection[F],
    queueKey: String,
    pollingInterval: FiniteDuration,
    pushEntry: String => F[Long]
  ) extends Queue[F, String] {

    def offer(a: String): F[Unit] = pushEntry(a).void
    def tryOffer(a: String): F[Boolean] = offer(a).as(true)
  
    // Members declared in cats.effect.std.QueueSource
    def size: F[Int] = RedisCommands.llen(queueKey).run(redisConnection).map(_.toInt)
    def take: F[String] = tryTake.flatMap{
      case Some(s) => s.pure[F]
      case _ => Temporal[F].sleep(pollingInterval) >> take
    }
      
    def tryTake: F[Option[String]] = RedisCommands.lpop(queueKey).run(redisConnection)

  
  }

  private class BoundedQueue[F[_]: Async](
    redisConnection: RedisConnection[F],
    queueKey: String,
    maxSize: Long,
    pollingInterval: FiniteDuration,
    pushEntry: String => F[Long]
  ) extends Queue[F, String]{
    def offer(a: String): F[Unit] = tryOffer(a).ifM(
      Applicative[F].unit,
      Temporal[F].sleep(pollingInterval) >> offer(a)
    )
    
    def tryOffer(a: String): F[Boolean] = 
      RedisCommands.llen(queueKey).run(redisConnection).flatMap{
        case exists if exists >= maxSize => false.pure[F]
        case otherwise => 
          pushEntry(a).as(true)
      }

    def size: F[Int] = RedisCommands.llen(queueKey).run(redisConnection).map(_.toInt)
    
    def take: F[String] = 
      tryTake.flatMap{
        case Some(s) => s.pure[F]
        case _ => Temporal[F].sleep(pollingInterval) >> take
      }
    
    def tryTake: F[Option[String]] = 
      RedisCommands.lpop(queueKey).run(redisConnection)  
  }




}