package io.chrisdavenport.rediculous.concurrent

import io.chrisdavenport.rediculous.RedisConnection
import fs2.concurrent.Queue

import cats._
import cats.syntax.all._
import cats.effect._
import fs2.Chunk
import io.chrisdavenport.rediculous.RedisCommands
import scala.concurrent.duration.FiniteDuration
import io.chrisdavenport.rediculous.RedisPipeline

object RedisQueue {

  /** rpush/lpop no size bound */
  def unboundedQueue[F[_]: Concurrent: Timer](
    redisConnection: RedisConnection[F],
    queueKey: String,
    pollingInterval: FiniteDuration
  ): Queue[F, String] = new RedisQueueUnboundedImpl[F](redisConnection, queueKey, pollingInterval,
    {s => RedisCommands.rpush(queueKey, List(s)).run(redisConnection)} 
  )

  /** lpush/lpop no size bound */
  def unboundedStack[F[_]: Concurrent: Timer](
    redisConnection: RedisConnection[F],
    queueKey: String,
    pollingInterval: FiniteDuration
  ): Queue[F, String] = new RedisQueueUnboundedImpl[F](redisConnection, queueKey, pollingInterval,
    {s => RedisCommands.lpush(queueKey, List(s)).run(redisConnection)} 
  )

  /** rpush/lpop size bound, polls on insert when full, can overfill */
  def boundedQueue[F[_]: Concurrent: Timer](
    redisConnection: RedisConnection[F],
    queueKey: String,
    maxSize: Long, 
    pollingInterval: FiniteDuration
  ): Queue[F, String] = new BoundedQueue[F](redisConnection, queueKey, maxSize, pollingInterval, 
    {s => RedisCommands.rpush(queueKey, List(s)).run(redisConnection)}
  )

    /** lpush/lpop size bound, polls on insert when full, can overfill */
  def boundedStack[F[_]: Concurrent: Timer](
    redisConnection: RedisConnection[F],
    queueKey: String,
    maxSize: Long, 
    pollingInterval: FiniteDuration
  ): Queue[F, String] = new BoundedQueue[F](redisConnection, queueKey, maxSize, pollingInterval, 
    {s => RedisCommands.lpush(queueKey, List(s)).run(redisConnection)}
  )

  private class RedisQueueUnboundedImpl[F[_]: Concurrent: Timer](
    redisConnection: RedisConnection[F],
    queueKey: String,
    pollingInterval: FiniteDuration,
    pushEntry: String => F[Long]
  ) extends Queue[F, String] {
    def enqueue1(a: String): F[Unit] = 
      pushEntry(a).void
    
    def offer1(a: String): F[Boolean] = 
      enqueue1(a).as(true)
    
    def dequeue1: F[String] = 
      tryDequeue1.flatMap{
        case Some(s) => s.pure[F]
        case _ => Timer[F].sleep(pollingInterval) >> dequeue1
      }
    
    def tryDequeue1: F[Option[String]] = 
      RedisCommands.lpop(queueKey).run(redisConnection)
    
    /**
      * Returns a nonEmpty chunk whose size is the result of 
      * maxSize calls of lpop run in a pipelined request. Since each
      * size is a physical call to redis it is highly recommended this number stay
      * far lower than the domain of Int.
      */
    def dequeueChunk1(maxSize: Int): F[cats.Id[Chunk[String]]] = 
      RedisCommands.lpop[RedisPipeline](queueKey).replicateA(maxSize)
        .pipeline
        .run(redisConnection)
        .flatMap{l => 
          val x = l.flatten
          x match {
            case Nil => Timer[F].sleep(pollingInterval) >> dequeueChunk1(maxSize)
            case otherwise => Chunk.seq(otherwise).pure[F]
          }
        }

    override def dequeue: fs2.Stream[F,String] = fs2.Stream.repeatEval(dequeue1)
    
    def tryDequeueChunk1(maxSize: Int): F[Option[cats.Id[Chunk[String]]]] = 
      RedisCommands.lpop[RedisPipeline](queueKey).replicateA(maxSize)
        .pipeline
        .run(redisConnection)
        .map{l => 
          if (l.exists(_.isDefined)) Some(Chunk.seq(l.flatten))
          else None
        }
    
    def dequeueChunk(maxSize: Int): fs2.Stream[F,String] =
      fs2.Stream.repeatEval(dequeueChunk1(maxSize))
        .flatMap{
          case chunk if chunk.isEmpty => fs2.Stream.eval_(Timer[F].sleep(pollingInterval)) ++ dequeueChunk(maxSize)
          case chunk => fs2.Stream.chunk(chunk)
        }
    
    def dequeueBatch: fs2.Pipe[F,Int,String] = _.evalMap{i => dequeueChunk1(i)}.flatMap(fs2.Stream.chunk)
  }

  private class BoundedQueue[F[_]: Concurrent: Timer](
    redisConnection: RedisConnection[F],
    queueKey: String,
    maxSize: Long,
    pollingInterval: FiniteDuration,
    pushEntry: String => F[Long]
  ) extends Queue[F, String]{
    def enqueue1(a: String): F[Unit] = offer1(a).ifM(
      Applicative[F].unit,
      Timer[F].sleep(pollingInterval) >> enqueue1(a)
    )
    
    def offer1(a: String): F[Boolean] = 
      RedisCommands.llen(queueKey).run(redisConnection).flatMap{
        case exists if exists >= maxSize => false.pure[F]
        case otherwise => 
          pushEntry(a).as(true)
      }

    override def dequeue: fs2.Stream[F,String] = fs2.Stream.repeatEval(dequeue1)
    
    def dequeue1: F[String] = 
      tryDequeue1.flatMap{
        case Some(s) => s.pure[F]
        case _ => Timer[F].sleep(pollingInterval) >> dequeue1
      }
    
    def tryDequeue1: F[Option[String]] = 
      RedisCommands.lpop(queueKey).run(redisConnection)
    
    /**
      * Returns a nonEmpty chunk whose size is the result of 
      * maxSize calls of lpop run in a pipelined request. Since each
      * size is a physical call to redis it is highly recommended this number stay
      * far lower than the domain of Int.
      */
    def dequeueChunk1(maxSize: Int): F[cats.Id[Chunk[String]]] = 
      RedisCommands.lpop[RedisPipeline](queueKey).replicateA(maxSize)
        .pipeline
        .run(redisConnection)
        .flatMap{l => 
          val x = l.flatten
          x match {
            case Nil => Timer[F].sleep(pollingInterval) >> dequeueChunk1(maxSize)
            case otherwise => Chunk.seq(otherwise).pure[F]
          }
        }
    
    def tryDequeueChunk1(maxSize: Int): F[Option[cats.Id[Chunk[String]]]] = 
      RedisCommands.lpop[RedisPipeline](queueKey).replicateA(maxSize)
        .pipeline
        .run(redisConnection)
        .map{l => 
          if (l.exists(_.isDefined)) Some(Chunk.seq(l.flatten))
          else None
        }
    
    def dequeueChunk(maxSize: Int): fs2.Stream[F,String] =
      fs2.Stream.repeatEval(dequeueChunk1(maxSize))
        .flatMap{
          case chunk if chunk.isEmpty => fs2.Stream.eval_(Timer[F].sleep(pollingInterval)) ++ dequeueChunk(maxSize)
          case chunk => fs2.Stream.chunk(chunk)
        }
    
    def dequeueBatch: fs2.Pipe[F,Int,String] = _.evalMap{i => dequeueChunk1(1)}.flatMap(fs2.Stream.chunk)
    
  }




}