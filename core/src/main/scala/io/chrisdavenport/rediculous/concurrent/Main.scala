package io.chrisdavenport.rediculous.concurrent

import io.chrisdavenport.rediculous._
import cats.implicits._
import cats.effect._
import fs2.io.tcp._
// import java.net.InetSocketAddress
// import fs2._
import scala.concurrent.duration._
import cats.syntax.SetOps

object Main extends IOApp {

  implicit class LogOps(a: Any){
    def putStrLn: IO[Unit] = IO(println(a))
  }

  def run(args: List[String]): IO[ExitCode] = {
    val r = for {
      blocker <- Blocker[IO]
      sg <- SocketGroup[IO](blocker)
      // maxQueued: How many elements before new submissions semantically block. Tradeoff of memory to queue jobs. 
      // Default 1000 is good for small servers. But can easily take 100,000.
      // workers: How many threads will process pipelined messages.
      connection <- RedisConnection.queued[IO](sg, "localhost", 6379, maxQueued = 10000, workers = 1)
    } yield connection

    r.use{ connection => 
      RedisSemaphore.build(connection, "sem", 2L, 10.seconds, 10.milli).flatMap{
        sem => 

        sem.tryAcquire >>//.flatTap(_.putStrLn) >>
        sem.tryAcquire >>//.flatTap(_.putStrLn) >> //>>
        sem.tryAcquire.map(b => s"Final Try Acquire Attempt $b").flatTap(_.putStrLn) //>>
        // sem.release.replicateA(2)
      }
      // val lockName = "lock:foo"

      // RedisSemaphore.semaphoreWithLimitLock(connection, "semaphoretest", 2, 10.seconds).use

      // Lock.acquireLockWithTimeout(connection, "foo", 10.seconds, 10.seconds).use{
      //   name => 
      //   RedisCommands.get[Redis[IO, *]](lockName).run(connection).flatTap(_.putStrLn)
      // } >> RedisCommands.get[Redis[IO, *]](lockName).run(connection).flatTap(_.putStrLn)
      // RedisRef.lockedLocation[IO](connection, "foo", "bar", 10.seconds, 10.seconds, RedisCommands.SetOpts(Some(60), None, None, false)).flatMap{
      //   ref => 
      //   // ref.get.flatTap(_.putStrLn) >>
      //   ref.access.flatMap{
      //     case (now, f) => 
      //       val set = "washington heights 3"
      //       now.putStrLn >>
      //       ref.set("bap") >>
      //       f(set).flatTap(_.putStrLn)
      //   } >>
      //   ref.get.flatTap(_.putStrLn)
      // }
      // val limiter = RateLimiter.create(connection, 10, duration = 10.seconds)


      //   limiter.get("foo").flatTap(_.putStrLn) >> 
      //   limiter.rateLimit("foo").flatTap(_.putStrLn).replicateA(9)// >> 
        // Timer[IO].sleep(5.seconds) >> 
        // limiter.rateLimit("foo").attempt.flatTap(_.putStrLn) >>
        // limiter.get("foo").flatTap(_.putStrLn)
    }.as(ExitCode.Success)
  }

}