package io.chrisdavenport.rediculous.concurrent

import cats.effect._
import com.dimafeng.testcontainers.GenericContainer
import io.chrisdavenport.rediculous.RedisConnection
import cats.effect.Blocker
import fs2.io.tcp.SocketGroup

object resources {
  def redisContainer[F[_]: Sync]: Resource[F, GenericContainer] = 
    Resource.make(
      Sync[F].delay {
        val x = GenericContainer("redis", Seq(6379))
        x.container.start()
        x
      }
    )(r => Sync[F].delay(r.container.stop()) )

  def redisConnection(implicit CS: ContextShift[IO], T: Timer[IO]): Resource[IO, RedisConnection[IO]] = for {
    blocker <- Blocker[IO]
    sg <- SocketGroup[IO](blocker)
    container <- redisContainer[IO].map(_.container)
    connection <- RedisConnection.queued[IO](sg, container.getContainerIpAddress(), container.getMappedPort(6379), maxQueued = 10000, workers = 4)
  } yield connection
  

}