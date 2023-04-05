package io.chrisdavenport.rediculous.concurrent

import cats.syntax.all._
import cats.effect._
import io.chrisdavenport.rediculous._
import io.chrisdavenport.rediculous.RedisCtx.syntax.all._
import scala.concurrent.duration._
import cats.effect.std.MapRef

class RedisMapRef[F[_]: Async] (
  redisConnection: RedisConnection[F], 
  acquireTimeout: FiniteDuration,
  lockTimeout: FiniteDuration,
  setOpts: RedisCommands.SetOpts
) extends MapRef[F, String, Option[String]] {

  def apply(k: String): Ref[F,Option[String]] =
    new RedisRef.LockedRedisRef[F](redisConnection, k, acquireTimeout, lockTimeout, setOpts)

  def unsetKey(k: String): F[Unit] =
      apply(k).set(None)
  def setKeyValue(k: String, v: String): F[Unit] = 
    apply(k).set(v.some)
  def getAndSetKeyValue(k: String, v: String): F[Option[String]] = 
    apply(k).getAndSet(v.some)

  def updateKeyValueIfSet(k: String, f: String => String): F[Unit] = 
    apply(k).update{
      case None => None
      case Some(v) => f(v).some
    }

  def modifyKeyValueIfSet[B](k: String, f: String => (String, B)): F[Option[B]] =
    apply(k).modify {
      case None => (None, None)
      case Some(v) => 
        val (set, out) = f(v)
        (set.some, out.some)
    }

}

object RedisMapRef {

  def impl[F[_]: Async](
    redisConnection: RedisConnection[F], acquireTimeout: FiniteDuration, lockTimeout: FiniteDuration,
    setOpts: RedisCommands.SetOpts
  ): RedisMapRef[F] = 
    new RedisMapRef[F](redisConnection, acquireTimeout, lockTimeout, setOpts)

}