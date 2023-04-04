package io.chrisdavenport.rediculous.concurrent

import io.chrisdavenport.rediculous.RedisConnection
import cats.syntax.all._
import cats.effect._
import cats.effect.syntax.all._
import cats.effect.std.UUIDGen
import scala.concurrent.duration._
import io.chrisdavenport.rediculous.RedisCommands
import io.circe._


object RedisSingleFibered {

  // State of the Deferred Location
  sealed trait SingleFiberedState[A]
  object SingleFiberedState {

    case class Canceled[A]() extends SingleFiberedState[A]
    case class Completed[A](maybeValue: Option[A]) extends SingleFiberedState[A]
    case class Errored[A](message: String) extends SingleFiberedState[A]

    import io.circe.syntax._
    implicit def encoder[A: Encoder]: Encoder[SingleFiberedState[A]] = Encoder.instance[SingleFiberedState[A]]{
      case Canceled() => Json.obj("state" -> "canceled".asJson)
      case Errored(message) => Json.obj("state" -> "errored".asJson, "message" -> message.asJson)
      case Completed(maybeValue) =>
        Json.obj("state" -> "completed".asJson, "value" -> maybeValue.asJson)

    }

    implicit def decoder[A: Decoder]: Decoder[SingleFiberedState[A]] = Decoder.instance{
      h =>

      h.downField("state").as[String].flatMap{
        case "canceled" => Canceled[A]().pure[Either[DecodingFailure, *]]
        case "errored" => h.downField("message").as[String].map(Errored[A](_))
        case "completed" => h.downField("value").as[Option[A]].map(Completed(_))
      }
    }
  }

  def redisSingleFibered[F[_]: Async: UUIDGen, V: Codec](
    connection: RedisConnection[F],
    keyRef: String,
    acquireTimeoutRef: FiniteDuration,
    lockTimeoutRef: FiniteDuration,
    setOptsRef: RedisCommands.SetOpts,

    pollingIntervalDeferred: FiniteDuration,
    lifetimeDeferred: FiniteDuration,
    deferredNameSpace: String = "deferred",
  )(
    action: F[V],
  ): F[V] = {
    val ref = RedisRef.lockedOptionRef[F](
      connection,
      keyRef,
      acquireTimeoutRef,
      lockTimeoutRef,
      setOptsRef
    )

    def fromDeferredLocation(value: String) =
      RedisDeferred.fromKey(connection, value, pollingIntervalDeferred, lifetimeDeferred)
        .get
        .flatMap(s => io.circe.parser.parse(s).liftTo[F])
        .flatMap(json => json.as[SingleFiberedState[V]].liftTo[F])
        .flatMap{
          case SingleFiberedState.Canceled() => new RuntimeException("RedisSingleFibered Remote Action Cancelled").raiseError[F, V]
          case SingleFiberedState.Errored(message) => new RuntimeException(s"RedisSingleFibered Remote Action Failed: $message").raiseError[F, V]
          case SingleFiberedState.Completed(None) => new RuntimeException(s"RedisSingleFibered Remote Action Did Not Return Value: Did you use a monad transformer that does not return a value as a part of a succes condition?").raiseError[F, V]
          case SingleFiberedState.Completed(Some(value)) => value.pure[F]
        }

    def encodeOutcome(outcome: Outcome[F, Throwable, V]): F[String] = outcome match {
      case Outcome.Canceled() =>
        val state: SingleFiberedState[V] = SingleFiberedState.Canceled[V]()
        SingleFiberedState.encoder[V].apply(state).noSpaces.pure[F]
      case Outcome.Errored(e) =>
        val state : SingleFiberedState[V] = SingleFiberedState.Errored(Option(e.toString).getOrElse("Null returned for the SingleFiberedState error toString"))
        SingleFiberedState.encoder[V].apply(state).noSpaces.pure[F]
      case Outcome.Succeeded(fa) =>
        Ref[F].of[Option[V]](None).flatMap{ref =>
          fa.flatMap{ v =>
            ref.set(Some(v))
          } >>
          ref.get.map{
            v =>
              val state: SingleFiberedState[V] = SingleFiberedState.Completed(v)
              SingleFiberedState.encoder[V].apply(state).noSpaces
          }
        }
    }

    UUIDGen[F].randomUUID.flatMap{ id =>
      val key = s"$deferredNameSpace:$id"
      ref.get.flatMap{
        case Some(value) => fromDeferredLocation(value)
        case None => Concurrent[F].uncancelable(poll =>
          ref.modify{
            case None => (key.some, Option.empty[String])
            case Some(value) => (value.some, value.some)
          }.flatMap{
            case Some(value) => poll(fromDeferredLocation(value))
            case None => // All Us and Only us
              val deferred = RedisDeferred.fromKey(connection, key, pollingIntervalDeferred, lifetimeDeferred)
              poll(action)
                .guaranteeCase{ outcome =>
                  encodeOutcome(outcome).flatMap{ out =>
                    ref.set(None) >> deferred.complete(out).void
                  }
                }
          }
        )
      }
    }
  }

  def redisSingleFiberedFunction[F[_]: Async: UUIDGen, K, V: Codec](
    connection: RedisConnection[F],
    baseRef: String,
    acquireTimeoutRef: FiniteDuration,
    lockTimeoutRef: FiniteDuration,
    setOptsRef: RedisCommands.SetOpts,

    pollingIntervalDeferred: FiniteDuration,
    lifetimeDeferred: FiniteDuration,
    deferredNameSpace: String = "deferred",
  )(
    encodeKey: K => String,
    action: K => F[V],
  ): K => F[V] = {(k: K) =>
    redisSingleFibered[F, V](
      connection,
      s"$baseRef:${encodeKey(k)}",
      acquireTimeoutRef,
      lockTimeoutRef,
      setOptsRef,
      pollingIntervalDeferred,
      lifetimeDeferred,
      s"$deferredNameSpace:${encodeKey(k)}"
    )(action(k))
  }
}