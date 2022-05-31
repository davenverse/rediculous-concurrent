package io.chrisdavenport.rediculous.concurrent.http4s


import cats.syntax.all._
import cats.effect.kernel._
import io.chrisdavenport.mapref.MapRef
import org.http4s.client._
import scala.concurrent.duration._
import io.chrisdavenport.rediculous._
import io.chrisdavenport.circuit.Backoff
import io.chrisdavenport.rediculous.concurrent.RedisCircuit
import io.chrisdavenport.circuit.http4s.client.CircuitedClient
import org.http4s._
import io.chrisdavenport.circuit.CircuitBreaker
import io.chrisdavenport.circuit.CircuitBreaker.RejectedExecution
import io.chrisdavenport.circuit.http4s.client.CircuitedClient._

object RedisCircuitedClient {

  def apply[F[_]: Async](
    redisConnection: RedisConnection[F],
    circuitMaxFailures: Int = 50,
    circuitResetTimeout: FiniteDuration = 1.second,
    redisSetOpts: RedisCommands.SetOpts = RedisCommands.SetOpts.default.copy(setSeconds = 120L.some),
    redisAcquireTimeout: FiniteDuration = 5.seconds,
    redisLockDuration: FiniteDuration = 10.seconds,
    redisCircuitPrefix: String = "http4s-circuit-",
    circuitBackoff: FiniteDuration => FiniteDuration = Backoff.exponential,
    circuitMaxResetTimeout: Duration = 1.minute,
    circuitModifications: CircuitBreaker[Resource[F, *]] => CircuitBreaker[Resource[F, *]] = {(x: CircuitBreaker[Resource[F, *]]) => x},
    circuitTranslatedError: (Request[F], RejectedExecution, RequestKey) => Option[Throwable] = defaultTranslatedError[F, RequestKey](_, _, _),
    circuitShouldFail: (Request[F], Response[F]) => ShouldCircuitBreakerSeeAsFailure = defaultShouldFail[F](_, _)
  )(client: Client[F]): Client[F] = {
    val iState = RedisCircuit.keyCircuitState(redisConnection, redisAcquireTimeout, redisLockDuration, redisSetOpts)
    val state = contramapKeys(iState)(requestKey(_, redisCircuitPrefix))
    CircuitedClient.byMapRefAndKeyed[F, RequestKey](
      state,
      RequestKey.fromRequest(_),
      circuitMaxFailures,
      circuitResetTimeout,
      circuitBackoff,
      circuitMaxResetTimeout,
      circuitModifications,
      circuitTranslatedError,
      circuitShouldFail
    )(client)
  }


  private def requestKey(requestKey: RequestKey, prefix: String = "http4s-circuit-"): String = {
    prefix ++ requestKey.toString
  }

  private def contramapKeys[F[_], K1, K2, V](mapRef: MapRef[F, K1, V])(g: K2 => K1): MapRef[F, K2, V] = 
    new MapRef[F, K2, V]{
      def apply(k: K2): Ref[F, V] = mapRef(g(k))
    }
}