import io.chrisdavenport.rediculous.concurrent._
import io.chrisdavenport.rediculous._
import cats.implicits._
import cats.effect._
import fs2._
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
      connection <- RedisConnection.queued[IO](sg, "localhost", 6379, maxQueued = 10000, workers = 4)
    } yield connection

    r.use{ connection => 
      // val ref = RedisRef.liftedDefaultStorage(
      //   RedisRef.lockedOptionRef(connection, "ref-test", 1.seconds, 10.seconds, RedisCommands.SetOpts(None, None, None, false)),
      //   "0"
      // ).imap(_.toInt)(_.toString)
      // val action = ref.modify{x => 
      //   val y = x + 1
      //   (y,y)
      // }
      val now = IO.delay(System.currentTimeMillis().millis)
      def time[A](io: IO[A]): IO[A] = 
        (now, io, now).tupled.flatMap{
          case (begin, out, end) => 
            (end - begin).putStrLn.map(_ => out)
        }

        // Layered Cache
        val cache = RedisCache.instance(connection, "namespace1", RedisCommands.SetOpts(Some(60), None, None, false))
        val cache2 = RedisCache.instance(connection, "namespace2", RedisCommands.SetOpts(Some(60), None, None, false))
        val layered = RedisCache.layer(cache, cache2)

        RedisCommands.get[Redis[IO, *]]("namespace1:test3").run(connection).flatTap(_.putStrLn) >>
        cache2.insert("test3", "value1") >> 
        layered.lookup("test3").flatTap(_.putStrLn) >> 
        RedisCommands.get[Redis[IO, *]]("namespace1:test3").run(connection).flatTap(_.putStrLn) >>
        RedisCommands.get[Redis[IO, *]]("namespace2:test3").run(connection).flatTap(_.putStrLn)


      // val queue = RedisQueue.boundedStack(connection, "bounded-queue-test", 10, 10.millis)

      // queue.dequeueChunk1(100) >>
      // Stream.awakeDelay[IO](0.5.seconds).zipRight(
      //   Stream.iterate[IO, Int](1)(_ + 1)
      // )
      //   .evalMap(i => queue.enqueue1(i.toString()))
      //   .concurrently(
      //     Stream.awakeDelay[IO](1.second).zipRight(
      //       Stream.repeatEval(RedisCommands.lrange[Redis[IO, *]]("bounded-queue-test", 0, -1).run(connection))
      //         .evalMap(_.putStrLn)
      //     )
      //   )
      //   .concurrently(
      //     Stream.awakeDelay[IO](0.4.second).zipRight(
      //       Stream.repeatEval(queue.dequeue1)
      //         .evalMap(i => s"Removed: $i".putStrLn)
      //     )
      //   )
      //   .timeout(15.seconds)
      //   .compile.drain
      // RedisCountdownLatch.createOrAccess(
      //   connection,
      //   "test-countdown-latch", 
      //   5, 
      //   1.seconds, 
      //   10.seconds, 
      //   100.millis, 
      //   60.seconds, 
      //   RedisCommands.SetOpts(Some(60), None, None, false)
      // ).flatMap{
      //   latch => 
      //   def release2: IO[Unit] = latch.release >>
      //     RedisCommands.get[Redis[IO, *]]("test-countdown-latch").run(connection).flatMap(_.putStrLn) >>
      //     Timer[IO].sleep(2000.millis) >> release2

      //   RedisCommands.get[Redis[IO, *]]("test-countdown-latch").run(connection).flatMap(_.putStrLn) >> 
      //   time(IO.race(
      //     latch.await, 
      //     release2
      //   )).flatMap(_.putStrLn) >>
      //   RedisCommands.get[Redis[IO, *]]("test-countdown-latch").run(connection).flatMap(_.putStrLn) >>
      //   RedisCommands.get[Redis[IO, *]]("test-countdown-latch:gate").run(connection).flatMap(_.putStrLn)
      // }
      // action >>
      // time(
      //   // Stream
      //   (
      //     Stream.eval(action).repeat.take(10000)
      //   )//
      //     .compile
      //     .drain
      // ) >> 
      // ref.get.flatTap(_.putStrLn)
        
      // action.replicateA(100000).void
      // val queue = RedisQueue.unbounded(connection, "queue-test", 50.millis)
      // queue.dequeueChunk(1000)
      //   .chunks
      //   .observeAsync(100000)(_.evalMap(_.putStrLn))
      //   .concurrently(
          // Stream.awakeDelay[IO](100.millis).zipRight(
      //       (
      //       Stream.iterate(0)(_ + 1).covary[IO]
      //     ).map(value => 
      //       Stream.eval(queue.enqueue1(value.toString))
      //     ).parJoin(10)//.timeout(10.seconds)
      //   )
      //   .compile
      //   .drain
      //   .timeout(15.seconds)
      // val deferred = RedisDeferred.fromKey(connection, "deferred-test", 100.millis, 10.seconds)
      // time(IO.race(deferred.get, Timer[IO].sleep(0.5.seconds) >> deferred.complete("Amazing") >> Timer[IO].sleep(0.5.seconds))).flatTap(_.putStrLn)
      // RedisSemaphore.build(connection, "sem-test-1", 2L, 10.seconds, 10.milli).flatMap{
      //   sem => 

      //   sem.tryAcquire >>//.flatTap(_.putStrLn) >>
      //   sem.tryAcquire >>//.flatTap(_.putStrLn) >> //>>
      //   time(IO.race(sem.acquire, Timer[IO].sleep(0.5.seconds) >> sem.release >> Timer[IO].sleep(0.5.seconds)))
      //     .flatTap(_.putStrLn)
        
      //   // sem.release.replicateA(2)
      // }
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