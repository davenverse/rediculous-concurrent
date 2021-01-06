package io.chrisdavenport.rediculous.concurrent

import cats._
import cats.syntax.all._
import cats.effect._
import io.chrisdavenport.rediculous._
import io.chrisdavenport.rediculous.{RedisCommands, RedisConnection}
import io.chrisdavenport.rediculous.RedisTransaction.TxResult
import scala.concurrent.duration._
import io.chrisdavenport.rediculous.RedisPipeline
import java.util.UUID
import cats.data.NonEmptyList
import io.chrisdavenport.rediculous.RedisTransaction.TxResult.Success
import io.chrisdavenport.rediculous.RedisTransaction.TxResult.Aborted

object RedisSemaphore {

  class RedisBackedSemaphore[F[_]: Concurrent: Timer](
    redisConnection: RedisConnection[F],
    semname: String,
    limit: Long,
    timeout: FiniteDuration,
    ownedIdentifiers: Ref[F, List[UUID]]
  ){
    def tryAcquire: 
  }

  // def tryAcquire[F[_]: Concurrent: Timer](
  //   redisConnection: RedisConnection[F],
  //   semname: String,
  //   limit: Long,
  //   timeout: FiniteDuration
  // ): F[Boolean] = 
    // Resource.liftF(RedisLock.tryAcquireLock(redisConnection, semname,timeout, timeout)).flatMap{
    //   case Some(lockIdentifier) => 
    //     Resource.liftF(tryAcquireSemaphore(redisConnection, semname, limit, timeout)).flatMap{ 
    //       case Some(semIdentifier) => 
    //         Resource.liftF(RedisLock.shutdownLock(redisConnection, semname, lockIdentifier)) >> 
    //         Resource.make(Applicative[F].unit)(_ => releaseSemaphore(redisConnection, semname, semIdentifier))
    //           .as(true)
    //       case None => 
    //         Resource.liftF(RedisLock.shutdownLock(redisConnection, semname, lockIdentifier)).as(false)
    //     }
    //   case None => false.pure[Resource[F, *]]
    // }


  def semaphoreWithLimitNoLock[F[_]: Concurrent](
    redisConnection: RedisConnection[F],
    semname: String,
    limit: Long,
    timeout: FiniteDuration
  ): Resource[F, Boolean] = 
    Resource.make(tryAcquireSemaphore(redisConnection, semname, limit, timeout)){
      case None => Sync[F].unit
      case Some(identifier) => releaseSemaphore(redisConnection, semname, identifier)
    }.map(_.isDefined)


  private def tryAcquireSemaphore[F[_]: Concurrent](
    redisConnection: RedisConnection[F],
    semname: String,
    limit: Long,
    timeout: FiniteDuration
  ): F[Option[UUID]] = Concurrent[F].suspend{
      val now = System.currentTimeMillis().millis
      val random = UUID.randomUUID()
      val identifier = random.toString()

      val czset = semname ++ ":owner"
      val ctr = semname ++ ":counter"

      def cleanup = 
        RedisCommands.zrem[RedisTransaction](semname, List(identifier)) *>
        RedisCommands.zrem[RedisTransaction](czset, List(identifier))
      
      val getSemaphore = (
        RedisCommands.zremrangebyscore[RedisTransaction](semname, Double.NegativeInfinity, (now - timeout).toMillis.toDouble),
        RedisCtx[RedisTransaction].keyed[Long](semname, NonEmptyList.of("ZINTERSTORE", czset, "2", czset, semname)),
        RedisCommands.incr[RedisTransaction](ctr),
        // RedisCommands.zadd[RedisTransaction](semname, List((now.toMillis.toDouble, random.toString))),
        // RedisCommands.zrank[RedisTransaction](semname, random.toString())
      ).mapN{case (_, _, counter) => counter}.transact.run(redisConnection).flatMap{
        case TxResult.Success(counter) => 
              val transaction = RedisCommands.zadd[RedisTransaction](semname, List((now.toMillis.toDouble, identifier))) *>
              RedisCommands.zadd[RedisTransaction](czset, List((counter.toDouble, identifier))) *> 
              RedisCommands.zrank[RedisTransaction](czset, identifier)

              transaction.transact.run(redisConnection).flatMap{
                case TxResult.Success(value) => 
                  if (value < limit) Option(random).pure[F]
                  else cleanup.transact.run(redisConnection).void.as(Option.empty[UUID])
                case TxResult.Aborted => cleanup.transact.run(redisConnection).void.as(Option.empty[UUID])
                case TxResult.Error(value) => cleanup.transact.run(redisConnection).void.as(Option.empty[UUID])
              }
        case TxResult.Aborted => Option.empty[UUID].pure[F]
        case TxResult.Error(e) => new Throwable(s"getSemaphore encountered an error $e").raiseError[F, Option[UUID]]
      }

      getSemaphore
  }

  private def releaseSemaphore[F[_]: Concurrent](
    redisConnection: RedisConnection[F],
    semname: String,
    identifier: java.util.UUID
  ): F[Unit] = 
    (
      RedisCommands.zrem[RedisTransaction](semname, List(identifier.toString)) <*
      RedisCommands.zrem[RedisTransaction](semname ++ ":owner", List(identifier.toString))
    ).transact.run(redisConnection).void


}