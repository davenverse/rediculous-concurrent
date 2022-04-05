package io.chrisdavenport.rediculous.concurrent

import cats.syntax.all._
import io.chrisdavenport.rediculous.{RedisConnection, RedisTransaction}
import io.chrisdavenport.rediculous.RedisCommands.{zremrangebyscore, zadd, zcard, zrange, pexpire, ZAddOpts}
import cats.effect._
import io.chrisdavenport.rediculous.RedisTransaction.TxResult.{Aborted, Success, Error}
import cats.Applicative
import scala.concurrent.duration._
import io.chrisdavenport.rediculous.RedisCtx.syntax.all._

trait RedisRateLimiter[F[_]]{
  def get(id: String): F[RedisRateLimiter.RateLimitInfo]
  def getAndDecrement(id: String): F[RedisRateLimiter.RateLimitInfo]
  def rateLimit(id: String): F[RedisRateLimiter.RateLimitInfo]
}

object RedisRateLimiter {

  case class RateLimitInfo(
    remaining: Long, // Remaining attempts
    total: Long, // max rate allowed for interval
    reset: FiniteDuration, // Time until all permits have reset
  )

  case class RateLimited(namespace: String, info: RateLimitInfo) extends Throwable(s"RateLimiter with namespace $namespace failed") with scala.util.control.NoStackTrace

  def create[F[_]: Async](
    connection: RedisConnection[F],
    max: Long = 2500,
    duration: FiniteDuration = 3600000.milliseconds, // milliseconds
    namespace : String = "rediculous-rate-limiter"
  ): RedisRateLimiter[F] = new RedisRateLimiter[F] {

    def getInternal(id: String, remove: Boolean): F[RateLimitInfo] = Concurrent[F].delay{
      val key = s"${namespace}:${id}"

      val now = System.currentTimeMillis()
      val start = (now.millis - duration)
      val random = java.util.UUID.randomUUID() // UUID means we avoid overlap at matching milli precision

      val possibleAdd: RedisTransaction[Unit] = 
        if (remove) zadd[RedisTransaction](key, List((now.toDouble, now.toString ++ "-" ++ random.toString())), ZAddOpts(None, false, false)).void 
        else Applicative[RedisTransaction].unit

      val operations = 
        (
          zremrangebyscore[RedisTransaction](key, 0, start.toMillis.toDouble),
          possibleAdd, 
          zcard[RedisTransaction](key),
          zrange[RedisTransaction](key, 0, 0),
          zrange[RedisTransaction](key, -max, -max),
          pexpire[RedisTransaction](key, duration.toMillis)
        ).mapN{
          case (_, _, count, oldest, oldestInRange, _) =>
            val resetMillis = 
              oldestInRange.headOption // Oldest value is the set accepted, since both numbers are same, list can only be empty or 1
                .orElse(oldest.headOption) // Or the oldest of any value
                .map(_.dropRight(37)) // Remove UUID salt
                .map(_.toLong)
                .map(_ + duration.toMillis)
                .getOrElse(now)

            val remaining = if (count < max) max - count else 0
            val reset = resetMillis.millis - now.millis
            val total = max

            RateLimitInfo(
              remaining, // Rate Limit Remaining
              total, // Total Permits
              reset, // Time to reset
            )
        }

      operations.transact[F].run(connection).flatMap{
        case Success(value) => value.pure[F]
        case Aborted => Concurrent[F].raiseError[RateLimitInfo](new Throwable("Transaction Aborted"))
        case Error(value) =>  Concurrent[F].raiseError[RateLimitInfo](new Throwable(s"Transaction Raised Error $value"))
      }
    }.flatten

    def get(id: String): F[RateLimitInfo] = getInternal(id, false)

    def getAndDecrement(id: String): F[RateLimitInfo] = getInternal(id, true)

    def rateLimit(id: String): F[RateLimitInfo] = get(id).flatMap{
      case r@RateLimitInfo(0, _, _) => RateLimited(namespace, r).raiseError[F, RateLimitInfo]
      case _ => getAndDecrement(id)
    }
  }

}