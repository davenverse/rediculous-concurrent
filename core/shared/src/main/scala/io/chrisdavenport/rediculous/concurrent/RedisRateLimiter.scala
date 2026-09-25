package io.chrisdavenport.rediculous.concurrent

import cats.syntax.all._
import io.chrisdavenport.rediculous.{RedisConnection, RedisTransaction}
import io.chrisdavenport.rediculous.RedisCommands.{zremrangebyscore, zadd, zcard, zrange, zrem, pexpire, ZAddOpts}
import cats.effect._
import io.chrisdavenport.rediculous.RedisTransaction.TxResult.{Aborted, Success, Error}
import cats.Applicative
import scala.concurrent.duration._
import io.chrisdavenport.rediculous.RedisCtx.syntax.all._
import cats.effect.std.UUIDGen

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

    // UUID means we avoid overlap at matching milli precision
    def getInternalDetailed(id: String, remove: Boolean): F[(Long, RateLimitInfo, String)] = UUIDGen[F].randomUUID.flatMap(random => Concurrent[F].delay{
      val key = s"${namespace}:${id}"

      val now = System.currentTimeMillis()
      val start = (now.millis - duration)

      val member = now.toString ++ "-" ++ random.toString()

      val possibleAdd: RedisTransaction[Unit] =
        if (remove) zadd[RedisTransaction](key, List((now.toDouble, member)), ZAddOpts(None, false, false)).void
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

            (
              count,
              RateLimitInfo(
                remaining, // Rate Limit Remaining
                total, // Total Permits
                reset, // Time to reset
              ),
              member
            )
        }

      operations.transact[F].run(connection).flatMap{
        case Success(value) => value.pure[F]
        case Aborted => Concurrent[F].raiseError[(Long, RateLimitInfo, String)](new Throwable("Transaction Aborted"))
        case Error(value) =>  Concurrent[F].raiseError[(Long, RateLimitInfo, String)](new Throwable(s"Transaction Raised Error $value"))
      }
    }.flatten)

    def getInternal(id: String, remove: Boolean): F[RateLimitInfo] =
      getInternalDetailed(id, remove).map(_._2)

    def get(id: String): F[RateLimitInfo] = getInternal(id, false)

    def getAndDecrement(id: String): F[RateLimitInfo] = getInternal(id, true)

    /**
     * Claims a permit and reports whether the claim was within the limit.
     *
     * Reading the count and then consuming a permit as two round trips lets
     * concurrent callers all observe the same remaining count and all proceed,
     * admitting more than `max` in a window. Instead this consumes first -- the
     * add and the count happen in one transaction, so the count includes this
     * caller's own entry and is authoritative -- and decides afterwards.
     *
     * A caller that turns out to be over the limit hands its permit back, so a
     * rejected attempt does not permanently shrink the window. Between the add
     * and that removal other callers may see a slightly inflated count, which
     * errs toward rejecting rather than admitting.
     */
    def rateLimit(id: String): F[RateLimitInfo] =
      getInternalDetailed(id, true).flatMap{ case (count, info, member) =>
        if (count <= max) info.pure[F]
        else {
          val key = s"${namespace}:${id}"
          zrem[RedisTransaction](key, List(member))
            .transact[F]
            .run(connection)
            .attempt
            .void >> RateLimited(namespace, info).raiseError[F, RateLimitInfo]
        }
      }
  }

}