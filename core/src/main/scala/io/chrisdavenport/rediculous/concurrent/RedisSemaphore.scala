package io.chrisdavenport.rediculous.concurrent

import cats._
import cats.syntax.all._
import cats.effect._
import cats.effect.concurrent._
import io.chrisdavenport.rediculous._
import io.chrisdavenport.rediculous.{RedisCommands, RedisConnection}
import io.chrisdavenport.rediculous.RedisTransaction.TxResult
import scala.concurrent.duration._
import io.chrisdavenport.rediculous.RedisPipeline
import java.util.UUID
import cats.data.NonEmptyList
import io.chrisdavenport.rediculous.RedisTransaction.TxResult.Success
import io.chrisdavenport.rediculous.RedisTransaction.TxResult.Aborted

trait MiniSemaphore[F[_]]{
  def acquire: F[Unit]
  def tryAcquire: F[Boolean]
  def release: F[Unit]
  def withPermit[A](f: F[A]): F[A]
}

object MiniSemaphore {

  def fromSemaphore[F[_]](semaphore: Semaphore[F]): MiniSemaphore[F] = new MiniSemaphore[F]{
    def acquire: F[Unit] = semaphore.acquire
    
    def tryAcquire: F[Boolean] = semaphore.tryAcquire
    
    def release: F[Unit] = semaphore.release
    
    def withPermit[A](f: F[A]): F[A] = semaphore.withPermit(f)
  }

  def fromRedisSemaphore[F[_]: Sync](r: RedisSemaphore.RedisBackedSemaphore[F]): MiniSemaphore[F] = new MiniSemaphore[F]{
    def acquire: F[Unit] = r.acquire
    
    def tryAcquire: F[Boolean] = r.tryAcquire
    
    def release: F[Unit] = r.release
    
    def withPermit[A](f: F[A]): F[A] = r.withPermit.use(_ => f)
  }

}

object RedisSemaphore {

  def build[F[_]: Concurrent: Timer](
    redisConnection: RedisConnection[F],
    semname: String, // The Name of the semaphore, also operates as the base of the redis key
    limit: Long, // Total Number of Permits Allowed
    timeout: FiniteDuration, // Maximum Time A Semaphore Can Be Held For. Will remove durations older than this
    poll: FiniteDuration = 10.millis, // On acquire, how frequently
    lockAcquireTimeout: FiniteDuration = 1.second, // How long to wait on acquiring a lock
    lockTotalTimeout: FiniteDuration = 10.seconds, // How long a lock can be held total (Don't see why we'd hit this)
  ): F[RedisBackedSemaphore[F]] = Ref.of(List.empty[UUID]).map(
    new RedisBackedSemaphore[F](redisConnection, semname, limit, timeout, poll, lockAcquireTimeout, lockTotalTimeout, _)
  )

  class RedisBackedSemaphore[F[_]: Concurrent: Timer](
    redisConnection: RedisConnection[F],
    semname: String,
    limit: Long,
    timeout: FiniteDuration,
    poll: FiniteDuration,
    lockAcquireTimeout: FiniteDuration,
    lockTotalTimeout: FiniteDuration,
    ownedIdentifiers: Ref[F, List[UUID]]
  ){

    def acquire: F[Unit] = 
      Concurrent.timeout(
        tryAcquire.flatMap{
        case true => Applicative[F].unit
        case false => Timer[F].sleep(poll) >> acquire
      }, timeout)

    def tryAcquire: F[Boolean] = 
      RedisLock.tryAcquireLock(redisConnection, semname,lockAcquireTimeout, lockTotalTimeout)
        .flatMap{
          case Some(lockIdentifier) => 
            tryAcquireSemaphore(redisConnection, semname, limit, timeout).flatTap{_ => 
              RedisLock.shutdownLock(redisConnection, semname, lockIdentifier)
            }.flatMap{
              case None => false.pure[F]
              case Some(semaphoreIdentifier) => 
                ownedIdentifiers.update(l => semaphoreIdentifier :: l).as(true)
            }
            
          case None => false.pure[F]
        }

    def release: F[Unit] = ownedIdentifiers.modify{
      case Nil => (Nil, None)
      case x :: xs => (xs, Some(x))
    }.flatMap{
      case Some(id) => releaseSemaphore(redisConnection, semname, id)
      case None =>  
        Applicative[F].unit
    }

    def withPermit: Resource[F, Unit] = Resource.make(acquire)(_ => release)
  }

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
        RedisCtx[RedisTransaction].keyed[Long](semname, NonEmptyList.of("ZINTERSTORE", czset, "2", czset, semname, "WEIGHTS", "1", "0")),
        RedisCommands.incr[RedisTransaction](ctr),
      ).tupled.transact.run(redisConnection).flatMap{
        case TxResult.Success(s@(_, _, counter)) => 
          val transaction = 
            (
              RedisCommands.zadd[RedisTransaction](semname, List((now.toMillis.toDouble, identifier))),
              RedisCommands.zadd[RedisTransaction](czset, List((counter.toDouble, identifier))),
              RedisCommands.zrank[RedisTransaction](czset, identifier)
            ).tupled

          transaction.transact.run(redisConnection).flatMap{
            case TxResult.Success(value@(_, _, rank)) => 
              // println(
              //   s"""Removed ${s._1} elements from $semname
              //   |Stored ${s._2} elements into $czset
              //   |Counter now at ${s._3} at $ctr
              //   |
              //   |Added ${value._1} elements to $semname (${now.toMillis.toDouble}, ${identifier})
              //   |Added ${value._2} elements to $czset (${counter.toDouble}, ${identifier})
              //   |Current Rank of $czset is $rank
              //   |""".stripMargin
              // )
              if (rank < limit) Option(random).pure[F]
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