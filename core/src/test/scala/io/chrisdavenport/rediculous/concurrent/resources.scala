package io.chrisdavenport.rediculous.concurrent

import cats.effect._
import com.dimafeng.testcontainers.GenericContainer
import io.chrisdavenport.rediculous.RedisConnection
import fs2.io.net._
import com.comcast.ip4s._

object resources {
  def redisContainer[F[_]: Sync]: Resource[F, GenericContainer] = 
    Resource.make(
      Sync[F].delay {
        val x = GenericContainer("redis", Seq(6379))
        x.container.start()
        x
      }
    )(r => Sync[F].delay(r.container.stop()) )

  def redisConnection: Resource[IO, RedisConnection[IO]] = for {
    container <- redisContainer[IO].map(_.container)
    connection <- RedisConnection.queued[IO].withHost(Host.fromString(container.getContainerIpAddress()).get).withPort(Port.fromInt(container.getMappedPort(6379)).get).build
  } yield connection
  

}