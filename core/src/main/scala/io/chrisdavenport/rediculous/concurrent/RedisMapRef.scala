package io.chrisdavenport.rediculous.concurrent

import cats.syntax.all._
import cats.effect._
import cats.effect.concurrent._
import io.chrisdavenport.rediculous._
import scala.concurrent.duration._
import io.chrisdavenport.mapref.MapRef

class RedisMapRef[F[_]: Concurrent: Timer] (
  redisConnection: RedisConnection[F], 
  acquireTimeout: FiniteDuration,
  lockTimeout: FiniteDuration
) extends Function1[String, Ref[F, Option[String]]] {

  def apply(k: String): Ref[F,Option[String]] = 
    new RedisMapRef.LockedRedisRef[F](redisConnection, k, acquireTimeout, lockTimeout)

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

  def impl[F[_]: Concurrent: Timer](
    redisConnection: RedisConnection[F], acquireTimeout: FiniteDuration, lockTimeout: FiniteDuration
  ): RedisMapRef[F] = 
    new RedisMapRef[F](redisConnection, acquireTimeout, lockTimeout)

  class LockedRedisRef[F[_]: Concurrent: Timer](
    redisConnection: RedisConnection[F],
    key: String,
    acquireTimeout: FiniteDuration,
    lockTimeout: FiniteDuration
  ) extends Ref[F, Option[String]]{
    private def getLock: Resource[F, Unit] = Lock.acquireLockWithTimeout(redisConnection, key, acquireTimeout, lockTimeout)
    private def tryGetLock: Resource[F, Boolean] = Lock.tryAcquireLockWithTimeout(
      redisConnection,
      key, 
      acquireTimeout,
      lockTimeout
    )

    def get: F[Option[String]] = RedisCommands.get(key).run(redisConnection)
    
    def set(a: Option[String]): F[Unit] = getLock.use{_ => 
      a match {
        case Some(value) => RedisCommands.set(key, value).void.run(redisConnection)
        case None => RedisCommands.del(key).void.run(redisConnection)
      }
      
    }
    
    def access: F[(Option[String], Option[String] => F[Boolean])] = 
      get.map{initial => 
        (initial, {toSet => 
          tryGetLock.use{
            case true => 
              get.flatMap{now => 
                if (now === initial) 
                  toSet match {
                    case Some(a) => RedisCommands.set(key, a).void.run(redisConnection).as(true)
                    case None => RedisCommands.del(key).void.run(redisConnection).as(true)
                  }
                else false.pure[F]
              }
            case false => false.pure[F]
          }
        })
      }
    
    def tryUpdate(f: Option[String] => Option[String]): F[Boolean] = 
      tryModify({s: Option[String] => (f(s), ())}).map(_.isDefined)
      
    
    def tryModify[B](f: Option[String] => (Option[String], B)): F[Option[B]] = {
      tryGetLock.use{
        case true => 
          get.flatMap{init => 
            val (after, out) = f(init)
            after match {
              case Some(value) => RedisCommands.set(key, value).void.run(redisConnection).as(out.some)
              case None => RedisCommands.del(key).void.run(redisConnection).as(out.some)
            }
            
          }
        case false => Option.empty[B].pure[F]
      }
    }
    
    def update(f: Option[String] => Option[String]): F[Unit] = modify{s: Option[String] => (f(s), ())}
    
    def modify[B](f: Option[String] => (Option[String], B)): F[B] =  tryModify(f).flatMap{
      case Some(s) => s.pure[F]
      case None => modify(f)
    }
    
    def tryModifyState[B](state: cats.data.State[Option[String],B]): F[Option[B]] = 
      tryModify{s: Option[String] => state.run(s).value}
    
    def modifyState[B](state: cats.data.State[Option[String],B]): F[B] = 
      modify{s: Option[String] => state.run(s).value}
  }
}