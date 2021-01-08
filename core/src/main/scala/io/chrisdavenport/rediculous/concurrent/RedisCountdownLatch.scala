package io.chrisdavenport.rediculous.concurrent

import cats._
import cats.syntax.all._
import cats.effect._
import cats.effect.concurrent._
import io.chrisdavenport.rediculous._
import io.circe._
import io.circe.syntax._
import io.chrisdavenport.rediculous.concurrent.RedisCountdownLatch.Awaiting
import io.chrisdavenport.rediculous.concurrent.RedisCountdownLatch.Done
import scala.concurrent.duration._
import cats.syntax.TryOps
import cats.instances.finiteDuration

abstract class CountDownLatch[F[_]] { self =>

  /**
   * Release a latch, decrementing the remaining count and
   * releasing any fibers that are blocked if the count
   * reaches 0
   */
  def release: F[Unit]

  /**
   * Semantically block until the count reaches 0
   */
  def await: F[Unit]

  def mapK[G[_]](f: F ~> G): CountDownLatch[G] =
    new CountDownLatch[G] {
      def release: G[Unit] = f(self.release)
      def await: G[Unit] = f(self.await)
    }

}

object RedisCountdownLatch {

  def createOrAccess[F[_]: Concurrent: Timer](
    redisConnection: RedisConnection[F],
    key: String,
    latches: Int,
    acquireTimeout: FiniteDuration,
    lockTimeout: FiniteDuration,
    pollingInterval: FiniteDuration,
    deferredLifetime: FiniteDuration,
    setOpts: RedisCommands.SetOpts
  ): F[CountDownLatch[F]] = {
    val ref = RedisRef.liftedSimple(
      stateAtLocation(redisConnection, key, acquireTimeout, lockTimeout, setOpts),
      (Awaiting(latches, key ++ ":gate"): State[String])
    )

    val defRef: Ref[F, State[Deferred[F, Unit]]] = ref.imap{s => 
      translateState(redisConnection, pollingInterval, deferredLifetime)(s)
        .map(a => liftDeferred(a, key))
    }(ref => 
      ref.map(_=> key ++ ":gate")
    )

    ref.update(identity)
      .as(new ConcurrentCountDownLatch[F](defRef))
  }

  def accessAtKey[F[_]: Concurrent: Timer](
    redisConnection: RedisConnection[F],
    key: String,
    acquireTimeout: FiniteDuration,
    lockTimeout: FiniteDuration,
    pollingInterval: FiniteDuration,
    deferredLifetime: FiniteDuration,
    setOpts: RedisCommands.SetOpts
  ): CountDownLatch[F] = {
    val ref = stateAtLocation(redisConnection, key, acquireTimeout, lockTimeout, setOpts)
  
    val defRef: Ref[F, Option[State[Deferred[F, Unit]]]] = ref.imap{s => 
      s.map(s => 
        translateState(redisConnection, pollingInterval, deferredLifetime)(s)
        .map(a => liftDeferred(a, key))
      )
    }(ref => 
      ref.map(_.map(_ => key ++ ":gate"))
    )

    new PossiblyAbsentCountdownLatch[F](defRef, pollingInterval)
  }

  private class ConcurrentCountDownLatch[F[_]: Concurrent](state: Ref[F, State[Deferred[F, Unit]]])
      extends CountDownLatch[F] {

    override def release: F[Unit] =
      Concurrent[F].uncancelable {
        state.modify {
          case Awaiting(n, signal) =>
            if (n > 1) (Awaiting(n - 1, signal), Applicative[F].unit) else (Done(), signal.complete(()).void)
          case d @ Done() => (d, Applicative[F].unit)
        }.flatten
      }

    override def await: F[Unit] =
      state.get.flatMap {
        case Awaiting(_, signal) => signal.get
        case Done() => Applicative[F].unit
      }

  }

  private class PossiblyAbsentCountdownLatch[F[_]: Concurrent: Timer](state: Ref[F, Option[State[Deferred[F, Unit]]]], pollingInterval: FiniteDuration) extends CountDownLatch[F] {
    override def release: F[Unit] =
      Concurrent[F].uncancelable {
        state.modify {
          case Some(Awaiting(n, signal)) =>
            if (n > 1) (Awaiting(n - 1, signal).some, false.pure[F]) else (Done().some, signal.complete(()).void.as(false))
          case Some(d @ Done()) => (d.some, false.pure[F])
          case None => (None, true.pure[F])
        }.flatten
      }.ifM(
        Timer[F].sleep(pollingInterval) >> release, 
        Applicative[F].unit
      )

    override def await: F[Unit] =
      state.get.flatMap {
        case Some(Awaiting(_, signal)) => signal.get
        case Some(Done()) => Applicative[F].unit
        case None => Timer[F].sleep(pollingInterval) >> await
      }
  }

  def stateAtLocation[F[_]: Concurrent: Timer](
    redisConnection: RedisConnection[F],
    key: String,
    acquireTimeout: FiniteDuration,
    lockTimeout: FiniteDuration,
    setOpts: RedisCommands.SetOpts
  ): Ref[F, Option[State[String]]] = 
    RedisRef.lockedOptionRef(redisConnection, key, acquireTimeout, lockTimeout, setOpts)
      .imap(_.map{i => 
        val json = i
        parser.parse(i).flatMap{
          _.as[State[String]]
        }
          .fold(e => throw new Throwable(s"State Is Not Correctly Formed - $e"), identity)
      })(_.map(_.asJson.noSpaces))

  sealed trait State[A] {
    def map[B](f: A => B): State[B] = this match {
      case Awaiting(latches, signalKey) => Awaiting(latches, f(signalKey))
      case Done() => Done()
    }
  }
  case class Awaiting[A](latches: Int, signalKey: A) // Turn into Deferred 
      extends State[A]
  case class Done[A]() extends State[A]

  def translateState[F[_]: Concurrent: Timer](
    redisConnection: RedisConnection[F],
    
    // keyLocation: String,
    pollingInterval: FiniteDuration,
    lifetime: FiniteDuration
  )(state: State[String]): State[Deferred[F, String]] = state match {
    case a@Awaiting(latches, signalKey) => 
      println(a)
      Awaiting(latches, 
        RedisDeferred.fromKey(redisConnection, signalKey, pollingInterval, lifetime)
      )
    case Done() => Done()
  }




  def liftDeferred[F[_]: Functor, A](
    tryAble: Deferred[F, A],
    default: A 
  ): Deferred[F, Unit] = new TranslatedDeferred[F, A](tryAble, default)

  def liftTryableDeferred[F[_]: Functor, A](
    tryAble: TryableDeferred[F, A],
    default: A 
  ): TryableDeferred[F, Unit] = new TranslatedTryDeferred[F, A](tryAble, default)

  class TranslatedTryDeferred[F[_]: Functor, A](
    val tryAble: TryableDeferred[F, A],
    val default: A
  ) extends TryableDeferred[F, Unit]{
    def complete(a: Unit): F[Unit] = 
      tryAble.complete(default)
    def get: F[Unit] = 
      tryAble.get.void
    def tryGet: F[Option[Unit]] = 
      tryAble.tryGet.map(_.void)
  }

  class TranslatedDeferred[F[_]: Functor, A](
    val tryAble: Deferred[F, A],
    val default: A
  ) extends Deferred[F, Unit]{
    def complete(a: Unit): F[Unit] = 
      tryAble.complete(default)
    def get: F[Unit] = 
      tryAble.get.void
  }


  implicit val encoder: Encoder[State[String]] = Encoder.instance[State[String]]{
    case Awaiting(latches, signalKey) => Json.obj(
        "state" -> "Awaiting".asJson,
        "latches" -> latches.asJson,
        "signal" -> signalKey.asJson
    )
    case Done() => Json.obj(
      "state" -> "Done".asJson
    )
  }

  implicit val decoder: Decoder[State[String]] = new Decoder[State[String]]{ 
    def apply(h: HCursor): Decoder.Result[State[String]] =  {
      for {
        state <- h.downField("state").as[String]
        out <- state match {
          case "Awaiting" => (
            h.downField("latches").as[Int],
            h.downField("signal").as[String]
          ).mapN(Awaiting(_, _): State[String])
          case "Done" => Either.right(Done(): State[String])
          case otherwise => Either.left(DecodingFailure(s"Incorrect State - $otherwise", List()))
        }
      } yield out
    }
  }

}


