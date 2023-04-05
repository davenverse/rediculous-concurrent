package io.chrisdavenport.rediculous.concurrent

import cats._
import io.chrisdavenport.rediculous.RedisConnection
import cats.syntax.all._
import cats.effect._
import cats.effect.syntax.all._
import cats.effect.std.UUIDGen
import scala.concurrent.duration._
import io.chrisdavenport.rediculous.RedisCommands
import io.circe._
import io.chrisdavenport.rediculous._


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

  /**
   * This leverages redis to do distributed action coordination.
   *
   * It assumes all participants will follow basic rules.
   * 1. The person holding the lock is the only person who gets to write to the keyLocation location
   * 2. When a person is done with an action, they will delete the keyLocation and release the lock (this implies on acquisition of the lock)
   *
   * This means any point can wait an entire maximumActionDuration to get an error if a crash occurs directly after the lock has been
   * acquired and the action proceeds.
   *
   * The data is stored leveraging json and the V should be able to traverse Encoding/Decoding with no loss
   * of information.
   *
   * Errors that occur will be propagated, but without access to the original class information.
   *
   * @param connection The redis connection to communicate using
   * @param keyLocation The location that the moving reference point uses
   * @param maximumActionDuration The maximum amount of time the action F[V] is allowed to take.
   *  This is something to put a lof of thought into as its also the maximum amount of time that any particular
   *  unit can wait.
   * @param acquireTimeoutKeyLocationLock This is the acquisition period that can occur when attempting to get the lock if redis is non-responsive.
   * @param pollingIntervalForCompletion This is how frequently each enqueued fiber is checking for the completion state.
   * @param deferredNamespace This is the namespace for temporary locations of information of completed actions. Since they need to outlive
   * the locks of individual actions for those who are in poll loops or received the key near simultaneously with keyLocation clearing.
   * @param action The action to perform if we become the owner of the shared action.
   **/
  def redisSingleFibered[F[_]: Async: UUIDGen, V: Decoder: Encoder](
    connection: RedisConnection[F],
    keyLocation: String,

    maximumActionDuration: FiniteDuration,

    acquireTimeoutKeyLocationLock: FiniteDuration,

    pollingIntervalForCompletion: FiniteDuration,
    deferredNameSpace: String = "deferred",
  )(
    action: F[V],
  ): F[V] = {
    def loop = redisSingleFibered(connection, keyLocation, maximumActionDuration, acquireTimeoutKeyLocationLock, pollingIntervalForCompletion, deferredNameSpace)(action)

    def fromDeferredLocation(key: String) =
      RedisDeferred.fromKey(connection, key, pollingIntervalForCompletion, maximumActionDuration)
        .get
        .flatMap(s => io.circe.parser.parse(s).liftTo[F])
        .flatMap(json => json.as[SingleFiberedState[V]].liftTo[F])
        .flatMap{
          case SingleFiberedState.Canceled() => new RuntimeException("RedisSingleFibered Remote Action Cancelled").raiseError[F, V]
          case SingleFiberedState.Errored(message) => new RuntimeException(s"RedisSingleFibered Remote Action Failed: $message").raiseError[F, V]
          case SingleFiberedState.Completed(None) => new RuntimeException(s"RedisSingleFibered Remote Action Did Not Return Value: Did you use a monad transformer that does not return a value as a part of a succes condition?").raiseError[F, V]
          case SingleFiberedState.Completed(Some(value)) =>
            value.pure[F]
        }.timeout(maximumActionDuration)

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

    def writeMaybe: Resource[F, Boolean] =
      RedisLock.tryAcquireLockWithTimeout(
        connection,
        keyLocation,
        acquireTimeout = acquireTimeoutKeyLocationLock,
        lockTimeout = maximumActionDuration,
      )

    UUIDGen[F].randomUUID.flatMap{ case id =>
      val key = s"$deferredNameSpace:$id"
      RedisCommands.get(keyLocation).run(connection).flatMap{
        case Some(value) =>
          fromDeferredLocation(value)
        case None =>
          Clock[F].realTime.flatMap{ start =>
            Concurrent[F].uncancelable(poll =>
              writeMaybe.use{
                case true =>
                  Clock[F].realTime.flatMap{ newTime =>
                    val elapsed = newTime - start
                    val setOpts = RedisCommands.SetOpts(
                      setSeconds = None,
                      setMilliseconds = Some((maximumActionDuration - elapsed).toMillis),
                      setCondition = Some(RedisCommands.Condition.Nx),
                      keepTTL = false
                    )
                    RedisCommands.set(keyLocation, key, setOpts).run(connection).flatMap{
                      case Some(value) => // We set the value so own the action
                        val deferred = RedisDeferred.fromKey(connection, key, pollingIntervalForCompletion, maximumActionDuration)
                        Clock[F].realTime.flatMap{ newTime =>
                          val elapsed = newTime - start
                          val rest = maximumActionDuration - elapsed
                          poll(action.timeout(rest))
                            .guaranteeCase{ outcome =>
                              encodeOutcome(outcome).flatMap{ out =>
                                Clock[F].realTime.flatMap{ endTime =>
                                  val elapsed = endTime - start
                                  val time = maximumActionDuration - elapsed
                                  val isExpired = time < 0.millis

                                  RedisCommands.del(keyLocation) // deletes your ownership run of the location in question
                                    .run(connection)
                                    .whenA(!isExpired) >> // this condition makes it so you don't delete someone elses ownership run that follows yours
                                    deferred.complete(out).void
                                }
                              }
                            }
                        }
                      case None =>
                        // Maybe something more specific
                        // Value should not exist when lock is acquired
                        new RuntimeException("Rediculous-Concurrent Invariant Break: Lock ownership established but value present").raiseError[F, V]
                    }
                  }
                // Dod not acquire lock, means someone else must have and can go get their value
                // resource has no finalizer since we did not acquire the lock
                case false => poll(loop)
              }
            )
          }
      }
    }
  }

  /**
   * This leverages redis to do distributed keyed action coordination.
   *
   * It assumes all participants will follow basic rules.
   * 1. The person holding the lock is the only person who gets to write to the keyLocation location
   * 2. When a person is done with an action, they will delete the keyLocation and release the lock (this implies on acquisition of the lock)
   *
   * This means any point can wait an entire maximumActionDuration to get an error if a crash occurs directly after the lock has been
   * acquired and the action proceeds.
   *
   * The data is stored leveraging json and the V should be able to traverse Encoding/Decoding with no loss
   * of information.
   *
   * Errors that occur will be propagated, but without access to the original class information.
   *
   * @param connection The redis connection to communicate using
   * @param baseKeyLocation The base namespace that each keyed moving reference point uses.
   * @param maximumActionDuration The maximum amount of time the action F[V] is allowed to take.
   *  This is something to put a lof of thought into as its also the maximum amount of time that any particular
   *  unit can wait.
   * @param acquireTimeoutKeyLocationLock This is the acquisition period that can occur when attempting to get the lock if redis is non-responsive.
   * @param pollingIntervalForCompletion This is how frequently each enqueued fiber is checking for the completion state.
   * @param deferredNamespace This is the namespace for temporary locations of information of completed actions. Since they need to outlive
   * the locks of individual actions for those who are in poll loops or received the key near simultaneously with keyLocation clearing.
   * @param encodeKey The function to encode keys into the namespace. Must be unique as this key encoding will be the equality reference
   * for what actions receive the same data.
   * @param action The action to perform if we become the owner of the shared action.
   **/
  def redisSingleFiberedFunction[F[_]: Async: UUIDGen, K, V: Decoder: Encoder](
    connection: RedisConnection[F],
    baseKeyLocation: String,

    maximumActionDuration: FiniteDuration,
    acquireTimeoutKeyLocationLock: FiniteDuration,

    pollingIntervalForCompletion: FiniteDuration,
    deferredNameSpace: String = "deferred",
  )(
    encodeKey: K => String,
    action: K => F[V],
  ): K => F[V] = {(k: K) =>
    redisSingleFibered[F, V](
      connection,
      s"$baseKeyLocation:${encodeKey(k)}",
      maximumActionDuration,
      acquireTimeoutKeyLocationLock,
      pollingIntervalForCompletion,

      s"$deferredNameSpace:${baseKeyLocation}:${encodeKey(k)}"
    )(action(k))
  }
}