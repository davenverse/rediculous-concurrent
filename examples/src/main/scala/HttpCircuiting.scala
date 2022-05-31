import io.chrisdavenport.rediculous.concurrent._
import io.chrisdavenport.rediculous._
import cats.syntax.all._
import cats.effect._
import scala.concurrent.duration._
import com.comcast.ip4s._
import _root_.io.chrisdavenport.mapref.MapRef
import org.http4s.client.RequestKey
import org.http4s.ember.client.EmberClientBuilder
import io.chrisdavenport.rediculous.concurrent.http4s.RedisCircuitedClient

object HttpCircuiting extends ResourceApp.Simple {

  def run: Resource[IO, Unit] = for {
    connection <- RedisConnection.queued[IO].withHost(host"localhost").withPort(port"6379").build
    automaticallyCircuitedClient <- EmberClientBuilder.default[IO].build
      .map(RedisCircuitedClient(connection)(_))
  } yield ()


}