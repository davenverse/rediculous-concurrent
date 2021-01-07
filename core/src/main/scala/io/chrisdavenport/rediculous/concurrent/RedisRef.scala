package io.chrisdavenport.rediculous.concurrent

import cats.syntax.all._
import io.chrisdavenport.rediculous._
import cats.effect._
import cats.effect.concurrent._
import io.chrisdavenport.rediculous.RedisTransaction.TxResult.{Aborted, Success, Error}
import cats.Applicative
import scala.concurrent.duration._
import cats.data.NonEmptyList
import io.chrisdavenport.rediculous.RedisProtocol.Status


object RedisRef {

  /** This uses the simple WATCH approach which means there is higher contention on the resource
    * which under heavy concurrency situations a lot of retries. However if there is very little
    * concurrency will behave smoother
    */
  def atLocation[F[_]: Concurrent](redisConnection: RedisConnection[F], key: String, setIfAbsent: String): F[Ref[F, String]] = {
    RedisCommands.setnx[Redis[F, *]](key, setIfAbsent)
      .run(redisConnection)
      .as(new RedisRef[F](redisConnection, key))
  }

  class RedisRef[F[_]: Concurrent](
    redisConnection: RedisConnection[F],
    key: String,
  ) extends Ref[F, String]{
    def get: F[String] = RedisCommands.get(key).run(redisConnection).flatMap{
      case o => Sync[F].delay(o.get)
    }
    
    def set(a: String): F[Unit] = RedisCommands.set(key, a).void.run(redisConnection)
    
    def access: F[(String, String => F[Boolean])] = 
      (RedisCtx[RedisPipeline].keyed[Status](key, NonEmptyList.of("WATCH", key)) *> RedisCommands.get[RedisPipeline](key))
        .pipeline
        .run(redisConnection)
        .flatMap(o => Sync[F].delay(o.get))
        .map{init => 
          (init, {after => 
            RedisCommands.set[RedisTransaction](key, after)
            .transact
            .run(redisConnection)
            .flatMap{
              case Success(_) => true.pure[F]
              case Aborted => false.pure[F]
              case Error(value) => new Throwable(s"RedisRef.access encountered error $value").raiseError[F, Boolean]
            }
          })
          
        }
    
    def tryUpdate(f: String => String): F[Boolean] = 
      tryModify({s => (f(s), ())}).map(_.isDefined)
      
    
    def tryModify[B](f: String => (String, B)): F[Option[B]] = {
      (RedisCtx[RedisPipeline].keyed[Status](key, NonEmptyList.of("WATCH", key)) *> RedisCommands.get[RedisPipeline](key))
        .pipeline
        .run(redisConnection)
        .flatMap(o => Sync[F].delay(o.get))
        .flatMap{init => 
          val (after, out) = f(init)
          RedisCommands.set[RedisTransaction](key, after)
            .transact
            .run(redisConnection)
            .flatMap{
              case Success(_) => out.some.pure[F]
              case Aborted => Option.empty[B].pure[F]
              case Error(value) => new Throwable(s"RedisRef.tryModify encountered error $value").raiseError[F, Option[B]]
            }
        }
    }
    
    def update(f: String => String): F[Unit] = modify(s => (f(s), ()))
    
    def modify[B](f: String => (String, B)): F[B] =  tryModify(f).flatMap{
      case Some(s) => s.pure[F]
      case None => modify(f)
    }
    
    def tryModifyState[B](state: cats.data.State[String,B]): F[Option[B]] = 
      tryModify(s => state.run(s).value)
    
    def modifyState[B](state: cats.data.State[String,B]): F[B] = 
      modify(s => state.run(s).value)
  }

  /**
    * This uses a seperate lock specifically for each keyed resource. Guarding access to any behavior
    * involving writes requires first aquiring the lock.
    */
  def lockedLocation[F[_]: Concurrent: Timer](
    redisConnection: RedisConnection[F],
    key: String,
    default: String,
    acquireTimeout: FiniteDuration,
    lockTimeout: FiniteDuration,
    setOpts: RedisCommands.SetOpts
  ): F[Ref[F, String]] = {
    RedisCommands.setnx[Redis[F, *]](key, default)
      .run(redisConnection)
      .as(new LockedRedisRef[F](redisConnection, key, acquireTimeout, lockTimeout, setOpts))
      .map(new LiftedRefDefaultStorage(_, default))
  }
  def lockedOptionRef[F[_]: Concurrent: Timer](
    redisConnection: RedisConnection[F],
    key: String,
    acquireTimeout: FiniteDuration,
    lockTimeout: FiniteDuration,
    setOpts: RedisCommands.SetOpts
  ): Ref[F, Option[String]] = 
    new LockedRedisRef[F](redisConnection, key, acquireTimeout, lockTimeout, setOpts)


  // Always stores information in Some(x) format in the underlying ref
  def liftedSimple[F[_]: Sync](
    ref: Ref[F, Option[String]],
    default: String
  ): Ref[F, String] = ref.imap(_.getOrElse(default))(_.some)

  def liftedDefaultStorage[F[_]: Sync](
    ref: Ref[F, Option[String]],
    default: String 
  ): Ref[F, String] = new LiftedRefDefaultStorage[F](ref, default)

  /**
   * Operates with default and anytime default is present instead information is removed from underlying ref.
   **/
  private class LiftedRefDefaultStorage[F[_]: Sync](
    val ref: Ref[F, Option[String]],
    val default: String
  ) extends Ref[F, String]{
    def get: F[String] = ref.get.map(_.getOrElse(default))
    
    def set(a: String): F[Unit] = {
      if (a =!= default) ref.set(a.some)
      else ref.set(None)
    }
    
    def access: F[(String, String => F[Boolean])] = ref.access.map{
      case (opt, cb) => 
        (opt.getOrElse(default), {s: String => 
          if (s =!= default) cb(s.some)
          else cb(None)
        })
    }
    
    def tryUpdate(f: String => String): F[Boolean] = 
      tryModify{s => (f(s), ())}.map(_.isDefined)
    
    def tryModify[B](f: String => (String, B)): F[Option[B]] =
      ref.tryModify{opt => 
        val s = opt.getOrElse(default)
        val (after, out) = f(s)
        if (after =!= default) (after.some, out)
        else (None, out)
      }
    
    def update(f: String => String): F[Unit] = 
      modify(s => (s, ()))
    
    def modify[B](f: String => (String, B)): F[B] = 
      ref.modify{opt => 
        val a = opt.getOrElse(default)
        val (out, b) = f(a)
        if (out =!= default) (out.some, b)
        else (None, b)
      }
    
    def tryModifyState[B](state: cats.data.State[String,B]): F[Option[B]] = 
      tryModify{s => state.run(s).value}
    
    def modifyState[B](state: cats.data.State[String,B]): F[B] = 
      modify{s => state.run(s).value}
    
  }

  class LockedRedisRef[F[_]: Concurrent: Timer](
    redisConnection: RedisConnection[F],
    key: String,
    acquireTimeout: FiniteDuration,
    lockTimeout: FiniteDuration,
    setOpts: RedisCommands.SetOpts
  ) extends Ref[F, Option[String]]{
    private def getLock: Resource[F, Unit] = RedisLock.acquireLockWithTimeout(redisConnection, key, acquireTimeout, lockTimeout)
    private def tryGetLock: Resource[F, Boolean] = RedisLock.tryAcquireLockWithTimeout(
      redisConnection,
      key, 
      acquireTimeout,
      lockTimeout
    )

    def get: F[Option[String]] = RedisCommands.get(key).run(redisConnection)
    
    def set(a: Option[String]): F[Unit] = getLock.use{_ => 
      a match {
        case Some(value) => RedisCommands.set(key, value, setOpts).void.run(redisConnection)
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
                    case Some(a) => RedisCommands.set(key, a, setOpts).void.run(redisConnection).as(true)
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
              case Some(value) => RedisCommands.set(key, value, setOpts).void.run(redisConnection).as(out.some)
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