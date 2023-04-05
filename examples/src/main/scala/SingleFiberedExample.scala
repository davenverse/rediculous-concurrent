

import cats.effect._
import cats.syntax.all._
import cats.effect.syntax.all._
import io.chrisdavenport.rediculous.RedisConnection
import fs2.Stream
import scala.concurrent.duration._
import com.comcast.ip4s._

import io.chrisdavenport.rediculous.concurrent.RedisSingleFibered
import io.chrisdavenport.rediculous.RedisCommands



object SingleFiberedExample extends IOApp {

  def action(name: String): IO[String] = {
    Ref[IO].of(0).flatMap{ ref =>
      IO.println(s"Started: $name") >>
      Stream(name)
        .covary[IO]
        .repeat
        .evalTap(_ => ref.update(_ + 1))
        .compile
        .drain
        .map(_ => "Won't happen")
        .timeout(5.seconds)
        .handleErrorWith(_ => ref.get.map(i => s"$name:$i"))
    }
  }


  def run(args: List[String]): IO[ExitCode] = {
    import io.circe._
    RedisConnection.queued[IO].withHost(host"localhost").withPort(port"6379").build.use{ conn =>
      val singleFibered = RedisSingleFibered.redisSingleFibered[IO, String](
        conn,
        "myKeyidentity",
        10.seconds,
        10.seconds,
        20.seconds,

        10.millis,
        15.seconds,
      )(_)
      val base = List.iterate(0, 50)(_ + 1)
        .map(i => singleFibered(action(i.toString)))

      base.parSequence.flatMap{
        list => IO.println(s"$list")
      }


    }.as(ExitCode.Success)


  }
}