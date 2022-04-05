package io.chrisdavenport.rediculous.concurrent

import cats._
import cats.syntax.all._
import cats.effect._
import cats.effect.syntax.all._
import io.circe._
import io.circe.syntax._
import io.chrisdavenport.rediculous._
import io.chrisdavenport.rediculous.RedisCtx.syntax.all._
import scala.concurrent.duration._
import java.util.UUID

/**
 * A synchronization abstraction that allows a set of fibers
 * to wait until they all reach a certain point.
 *
 * A cyclic barrier is initialized with a positive integer capacity n and
 * a fiber waits by calling [[await]], at which point it is semantically
 * blocked until a total of n fibers are blocked on the same cyclic barrier.
 *
 * At this point all the fibers are unblocked and the cyclic barrier is reset,
 * allowing it to be used again.
 */
trait CyclicBarrier[F[_]]{ self => 
  /**
   * Possibly semantically block until the cyclic barrier is full
   */
  def await: F[Unit]

  def mapK[G[_]](f: F ~> G): CyclicBarrier[G] =
    new CyclicBarrier[G] {
      def await: G[Unit] = f(self.await)
    }
}

object RedisCyclicBarrier {

  def create[F[_]: Async](
    redisConnection: RedisConnection[F],
    key: String,
    capacity: Int,
    acquireTimeout: FiniteDuration,
    lockTimeout: FiniteDuration,
    pollingInterval: FiniteDuration,
    deferredLifetime: FiniteDuration, // Should be a long time
    setOpts: RedisCommands.SetOpts
  ): CyclicBarrier[F] = 
    new RedisCyclicBarrier[F](redisConnection, key, capacity, acquireTimeout, lockTimeout, pollingInterval, deferredLifetime, setOpts)


  case class State(awaiting: Int, epoch: Long, currentDeferredLocation: String)


  private class RedisCyclicBarrier[F[_]: Async](
    redisConnection: RedisConnection[F],
    key: String,
    capacity: Int,
    acquireTimeout: FiniteDuration,
    lockTimeout: FiniteDuration,
    pollingInterval: FiniteDuration,
    deferredLifetime: FiniteDuration,
    setOpts: RedisCommands.SetOpts
  ) extends CyclicBarrier[F]{
    val ref = RedisRef.optionJsonRef[F, State](RedisRef.lockedOptionRef(redisConnection, key, acquireTimeout, lockTimeout, setOpts))

    def keyLocation(uuid: UUID): String = key ++ ":lock:" ++ uuid.toString()

    def deferredAtLocation(location: String): Deferred[F, String] = RedisDeferred.fromKey(
      redisConnection,
      location,
      pollingInterval, 
      deferredLifetime
    )

    def await: F[Unit] = Sync[F].delay(UUID.randomUUID()).flatMap{ gate => 
      ref.modify{
        case Some(State(awaiting, epoch, location)) => 
          val awaitingNow = awaiting - 1
          if (awaitingNow == 0)
            Some(State(capacity, epoch + 1, keyLocation(gate))) -> deferredAtLocation(location).complete(key).void
          else {
            val newState = State(awaitingNow, epoch, location)
            // reincrement count if this await gets canceled,
            // but only if the barrier hasn't reset in the meantime
            val cleanup = ref.update { 
              case Some(s) =>
                if (s.epoch == epoch) Some(s.copy(awaiting = s.awaiting + 1))
                else Some(s)
              case None => None // Shouldn't end up here
            }

            Some(newState) -> deferredAtLocation(location).get.void.guaranteeCase{
              case Outcome.Canceled() => cleanup
              case _ => Applicative[F].unit
            }
          }
        case None =>
          (State(capacity, 0, keyLocation(gate)).some, await)
      }.flatten
    }
  }




  implicit val encoder: Encoder[State] = Encoder.instance[State]{
    case State(latches, epoch, location) => Json.obj(
        "latches" -> latches.asJson,
        "epoch" -> epoch.asJson,
        "location" -> location.asJson
    )
  }

  implicit val decoder: Decoder[State] = new Decoder[State]{ 
    def apply(h: HCursor): Decoder.Result[State] =  {
      for {
        latches <- h.downField("latches").as[Int]
        epoch <- h.downField("epoch").as[Long]
        location <- h.downField("location").as[String]
      } yield State(latches, epoch, location)
    }
  }

}

