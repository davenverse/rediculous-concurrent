package io.chrisdavenport.rediculous.concurrent

import io.chrisdavenport.rediculous.RedisConnection
import fs2.concurrent.Queue

import cats.syntax.all._
import cats.effect._
import fs2.Chunk
import io.chrisdavenport.rediculous.RedisCommands
import scala.concurrent.duration.FiniteDuration
import io.chrisdavenport.rediculous.RedisPipeline

object RedisQueue {

  def unbounded[F[_]: Concurrent: Timer](
    redisConnection: RedisConnection[F],
    queueKey: String,
    pollingInterval: FiniteDuration
  ): Queue[F, String] = new RedisQueueUnboundedImpl[F](redisConnection, queueKey, pollingInterval)

  private class RedisQueueUnboundedImpl[F[_]: Concurrent: Timer](
    redisConnection: RedisConnection[F],
    queueKey: String,
    pollingInterval: FiniteDuration
  ) extends Queue[F, String] {
    def enqueue1(a: String): F[Unit] = 
      RedisCommands.rpush(queueKey, List(a)).void.run(redisConnection)
    
    def offer1(a: String): F[Boolean] = 
      enqueue1(a).as(true)
    
    def dequeue1: F[String] = 
      tryDequeue1.flatMap{
        case Some(s) => s.pure[F]
        case _ => Timer[F].sleep(pollingInterval) >> dequeue1
      }
    
    def tryDequeue1: F[Option[String]] = 
      RedisCommands.lpop(queueKey).run(redisConnection)
    
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