import io.chrisdavenport.rediculous.concurrent._
import io.chrisdavenport.rediculous._
import cats.implicits._
import cats.effect._
import fs2._
import fs2.io.net._
// import java.net.InetSocketAddress
// import fs2._
import scala.concurrent.duration._
import cats.syntax.SetOps
import com.comcast.ip4s._
import _root_.io.chrisdavenport.mapref.MapRef
import org.http4s.client.RequestKey
import org.http4s.ember.client.EmberClientBuilder
import _root_.io.chrisdavenport.circuit.http4s.client.CircuitedClient
import _root_.io.chrisdavenport.circuit.CircuitBreaker

object HttpCircuiting extends ResourceApp.Simple {

  def run: Resource[IO, Unit] = for {
    connection <- RedisConnection.queued[IO].withHost(host"localhost").withPort(port"6379").build
    baseState = RedisCircuit.keyCircuitState(connection, 10.seconds, 10.seconds, RedisCommands.SetOpts.default.copy(setSeconds = 120L.some))
    state = contramapKeys[IO, String, RequestKey, Option[CircuitBreaker.State]](baseState)(requestKey(_))
    automaticallyCircuitedClient <- EmberClientBuilder.default[IO].build
      .map(CircuitedClient.byMapRefAndKeyed(state, RequestKey.fromRequest(_), maxFailures = 50, 1.second))
  } yield ()

  def requestKey(requestKey: RequestKey, prefix: String = "http4s-circuit-"): String = {
    prefix ++ requestKey.toString
  }

  def contramapKeys[F[_], K1, K2, V](mapRef: MapRef[F, K1, V])(g: K2 => K1): MapRef[F, K2, V] = 
    new MapRef[F, K2, V]{
      def apply(k: K2): Ref[F, V] = mapRef(g(k))
    }
}