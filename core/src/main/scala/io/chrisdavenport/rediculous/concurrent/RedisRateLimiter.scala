package io.chrisdavenport.rediculous.concurrent

import cats._
import cats.syntax.all._
import io.chrisdavenport.rediculous._
import io.chrisdavenport.rediculous.RedisCommands.{zremrangebyscore, zadd, zcard, zrange, pexpire, ZAddOpts}
import cats.effect._
import io.chrisdavenport.rediculous.RedisTransaction.TxResult.{Aborted, Success, Error}
import cats.Applicative
import scala.concurrent.duration._
import io.chrisdavenport.ratelimit.RateLimiter
import cats.data.Kleisli
import cats.data.NonEmptyList

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

  private def redisTime[F[_]: Async](redisConnection: RedisConnection[F]): Kleisli[F, FiniteDuration, *] ~> F = 
    new (Kleisli[F, FiniteDuration, *] ~> F){
      def apply[A](fa: Kleisli[F, FiniteDuration, A]): F[A] = 
        RedisCommands.time[Redis[F, *]].run(redisConnection).map(_._1.seconds).flatMap(fa.run)
    }

  private def temporalTime[F[_]: Temporal]: Kleisli[F, FiniteDuration, *] ~> F = 
    new (Kleisli[F, FiniteDuration, *] ~> F){
      def apply[A](fa: Kleisli[F, FiniteDuration, A]): F[A] = 
        Temporal[F].realTime.flatMap(fa.run)
    }

  def fixedWindow[F[_]: Async](
    connection: RedisConnection[F],
    maxRate: String => Long,
    periodSeconds: Long,
    namespace: String = "rediculous-rate-limiter",
    useRedisTime: Boolean = false,
  ): RateLimiter[F, String] = 
    new FixedWindow[F](connection, maxRate, periodSeconds, namespace).mapK{
      if (useRedisTime) redisTime(connection) else temporalTime
    }

  private class FixedWindow[F[_]: Async](
    connection: RedisConnection[F],
    maxRate: String => Long,
    periodSeconds: Long,
    namespace : String,
  ) extends RateLimiter[Kleisli[F, FiniteDuration, *], String]{

    val comment = RateLimiter.QuotaComment("comment", Either.right("fixed window"))
    def limit(k: String) = {
      val max = maxRate(k)
      RateLimiter.RateLimitLimit(max, RateLimiter.QuotaPolicy(max, periodSeconds, comment :: Nil) :: Nil)
    }

    def createRateLimit(secondsLeftInPeriod: Long, k: String, currentCount: Long): RateLimiter.RateLimit = {
      val reset = RateLimiter.RateLimitReset(secondsLeftInPeriod)
      val l = limit(k)
      val remain = l.limit - currentCount
      val remaining = RateLimiter.RateLimitRemaining(Math.max(remain, 0))
      RateLimiter.RateLimit(
        if (remain < 0) RateLimiter.WhetherToRateLimit.ShouldRateLimit else RateLimiter.WhetherToRateLimit.ShouldNotRateLimit,
        l,
        remaining,
        reset
      )
    }

    def get(id: String): Kleisli[F,FiniteDuration,RateLimiter.RateLimit] = Kleisli{(fd: FiniteDuration) =>
      val secondsSinceEpoch = fd.toSeconds
      val periodNumber = secondsSinceEpoch / periodSeconds
      val periodEndSeconds = (periodNumber + 1)  * periodSeconds
      val secondsLeftInPeriod = periodEndSeconds - secondsSinceEpoch
      RedisCommands.get(s"$namespace$id$periodNumber").map(_.map(_.toLong).getOrElse(0L)).run(connection).map(
        createRateLimit(secondsLeftInPeriod, id, _)
      )
    }
    
    def getAndDecrement(id: String): Kleisli[F,FiniteDuration,RateLimiter.RateLimit] = Kleisli{(fd: FiniteDuration) =>
      val secondsSinceEpoch = fd.toSeconds
      val periodNumber = secondsSinceEpoch / periodSeconds
      val periodEndSeconds = (periodNumber + 1)  * periodSeconds
      val secondsLeftInPeriod = periodEndSeconds - secondsSinceEpoch
      val key = s"$namespace$id$periodNumber"
      val pipeline = (
        RedisCommands.incr[RedisPipeline](key),
        io.chrisdavenport.rediculous.RedisCtx[RedisPipeline].keyed[Int](key, NonEmptyList.of("EXPIRE", key, (secondsLeftInPeriod + 1).toString(), "NX"))
      ).mapN{ case(count, _) => count}
      pipeline.pipeline[F].run(connection).map(
        createRateLimit(secondsLeftInPeriod, id, _)
      )
    }
    
    def rateLimit(id: String): Kleisli[F,FiniteDuration,RateLimiter.RateLimit] = getAndDecrement(id).flatMap{
      case r@RateLimiter.RateLimit(RateLimiter.WhetherToRateLimit.ShouldRateLimit, _, _, _) => 
        Kleisli.liftF(RateLimiter.FastRateLimited(id, r).raiseError[F, RateLimiter.RateLimit])
      case otherwise =>  Kleisli.liftF(otherwise.pure[F])
    }
    
  }


  def slidingWindow[F[_]: Async](
    connection: RedisConnection[F],
    maxRate: String => Long,
    periodSeconds: Long,
    namespace: String = "rediculous-rate-limiter",
    useRedisTime: Boolean = false,
  ): RateLimiter[F, String] = 
    new SlidingWindow[F](connection, maxRate, periodSeconds, namespace).mapK{
      if (useRedisTime) redisTime(connection) else temporalTime
    }

  private class SlidingWindow[F[_]: Async](
    connection: RedisConnection[F],
    maxRate: String => Long,
    periodSeconds: Long,
    namespace : String,
  ) extends RateLimiter[Kleisli[F, FiniteDuration, *], String]{

    val comment = RateLimiter.QuotaComment("comment", Either.right("sliding window"))
    def limit(k: String) = {
      val max = maxRate(k)
      RateLimiter.RateLimitLimit(max, RateLimiter.QuotaPolicy(max, periodSeconds, comment :: Nil) :: Nil)
    }

    def createRateLimit(secondsLeftInPeriod: Long, k: String, lastPeriodCount: Long, currentCount: Long): RateLimiter.RateLimit = {
      val l = limit(k)
      val percent = ((secondsLeftInPeriod - 1).toDouble / periodSeconds.toDouble)
      val fromLastPeriod = Math.floor(percent * lastPeriodCount).toLong
      val remain = l.limit - fromLastPeriod - currentCount
      val remaining = RateLimiter.RateLimitRemaining(Math.max(remain, 0))
      val reset = if (remain <= 0 && fromLastPeriod > 0) {
        def secondsTillNewPermitsFromLast(nextSecond: Int): RateLimiter.RateLimitReset = {
          val percent = (((secondsLeftInPeriod - 1).toDouble - nextSecond) / periodSeconds.toDouble)
          val newFromLastPeriod = Math.floor(percent * lastPeriodCount).toLong
          if (newFromLastPeriod < fromLastPeriod) {
            RateLimiter.RateLimitReset(nextSecond.toLong)
          } else if (nextSecond + secondsLeftInPeriod >= periodSeconds){
            RateLimiter.RateLimitReset(secondsLeftInPeriod)
          } else secondsTillNewPermitsFromLast(nextSecond + 1)
        }
        secondsTillNewPermitsFromLast(1)
      } else {
        RateLimiter.RateLimitReset(secondsLeftInPeriod)
      }

      RateLimiter.RateLimit(
        if (remain < 0) RateLimiter.WhetherToRateLimit.ShouldRateLimit else RateLimiter.WhetherToRateLimit.ShouldNotRateLimit,
        l,
        remaining,
        reset
      )
    }

    def get(id: String): Kleisli[F,FiniteDuration,RateLimiter.RateLimit] = Kleisli{(fd: FiniteDuration) =>
      val secondsSinceEpoch = fd.toSeconds
      val periodNumber = secondsSinceEpoch / periodSeconds
      val periodEndSeconds = (periodNumber + 1)  * periodSeconds
      val secondsLeftInPeriod = periodEndSeconds - secondsSinceEpoch
      (
        RedisCommands.get[RedisPipeline](s"$namespace$id${periodNumber - 1}").map(_.map(_.toLong).getOrElse(0L)),
        RedisCommands.get[RedisPipeline](s"$namespace$id$periodNumber").map(_.map(_.toLong).getOrElse(0L))
      ).tupled.pipeline[F].run(connection)
        .map{ case (last, current) => 
          createRateLimit(secondsLeftInPeriod, id, last, current)
        }
    }
    
    def getAndDecrement(id: String): Kleisli[F,FiniteDuration,RateLimiter.RateLimit] = Kleisli{(fd: FiniteDuration) =>
      val secondsSinceEpoch = fd.toSeconds
      val periodNumber = secondsSinceEpoch / periodSeconds
      val periodEndSeconds = (periodNumber + 1)  * periodSeconds
      val secondsLeftInPeriod = periodEndSeconds - secondsSinceEpoch
      val key = s"$namespace$id$periodNumber"
      (
        RedisCommands.get[RedisPipeline](s"$namespace$id${periodNumber - 1}").map(_.map(_.toLong).getOrElse(0L)),
        RedisCommands.incr[RedisPipeline](key),
        io.chrisdavenport.rediculous.RedisCtx[RedisPipeline].keyed[Int](key, NonEmptyList.of("EXPIRE", key, (secondsLeftInPeriod + 1).toString(), "NX"))
      ).mapN{ case(last, current, _) =>
        createRateLimit(secondsLeftInPeriod, id, last, current)
      }.pipeline.run(connection)
    }
    
    def rateLimit(id: String): Kleisli[F,FiniteDuration,RateLimiter.RateLimit] = getAndDecrement(id).flatMap{
      case r@RateLimiter.RateLimit(RateLimiter.WhetherToRateLimit.ShouldRateLimit, _, _, _) => 
        Kleisli.liftF(RateLimiter.FastRateLimited(id, r).raiseError[F, RateLimiter.RateLimit])
      case otherwise =>  Kleisli.liftF(otherwise.pure[F])
    }
  }
}