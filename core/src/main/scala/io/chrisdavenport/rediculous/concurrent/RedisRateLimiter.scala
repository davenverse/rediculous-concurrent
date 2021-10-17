package io.chrisdavenport.rediculous.concurrent

import cats.syntax.all._
import io.chrisdavenport.rediculous.{RedisConnection, RedisTransaction}
import io.chrisdavenport.rediculous.RedisCommands.{zremrangebyscore, zadd, zcard, zrange, pexpire, ZAddOpts}
import cats.effect._
import io.chrisdavenport.rediculous.RedisTransaction.TxResult.{Aborted, Success, Error}
import cats.Applicative
import scala.concurrent.duration._
import io.chrisdavenport.ratelimit.RateLimiter

object RedisRateLimiter {

  def slidingLog[F[_]: Async](
    connection: RedisConnection[F],
    max: Long = 2500,
    duration: FiniteDuration = 3600000.milliseconds, // milliseconds
    namespace : String = "rediculous-rate-limiter"
  ): RateLimiter[F, String] = new RateLimiter[F, String] {

    def getInternal(id: String, remove: Boolean): F[RateLimiter.RateLimit] = Concurrent[F].delay{
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

            RateLimiter.RateLimit(
              whetherToRateLimit = if (count > max) RateLimiter.WhetherToRateLimit.ShouldRateLimit else RateLimiter.WhetherToRateLimit.ShouldNotRateLimit,
              remaining = RateLimiter.RateLimitRemaining(remaining.toLong), // Rate Limit Remaining
              limit = RateLimiter.RateLimitLimit(total, List.empty), // Total Permits
              reset = RateLimiter.RateLimitReset(reset.toSeconds), // Time to reset
            )
        }

      operations.transact[F].run(connection).flatMap{
        case Success(value) => value.pure[F]
        case Aborted => Concurrent[F].raiseError[RateLimiter.RateLimit](new Throwable("Transaction Aborted"))
        case Error(value) =>  Concurrent[F].raiseError[RateLimiter.RateLimit](new Throwable(s"Transaction Raised Error $value"))
      }
    }.flatten

    def get(id: String): F[RateLimiter.RateLimit] = getInternal(id, false)

    def getAndDecrement(id: String): F[RateLimiter.RateLimit] = getInternal(id, true)

    def rateLimit(id: String): F[RateLimiter.RateLimit] = getAndDecrement(id).flatMap{
      case r@RateLimiter.RateLimit(RateLimiter.WhetherToRateLimit.ShouldRateLimit, _, _, _) => 
        RateLimiter.FastRateLimited(id, r).raiseError[F, RateLimiter.RateLimit]
      case otherwise => otherwise.pure[F]
    }
  }

}