package io.chrisdavenport.rediculous.concurrent

import cats.syntax.all._
import cats._
import cats.conversions._
import cats.effect._
import io.chrisdavenport.mules._
import io.chrisdavenport.rediculous._
import cats.effect.syntax.all._
import cats.data.Func

object RedisCache {

  

  /** Layering Function Allows you to put a In-Memory Cache
   *
   * Lookups start at the top layer, and if present don't go further. If absent progresses
   * to the lower cache and will insert into the top layer before the value is returned
   * 
   * inserts and deletes are proliferated to top and then bottom
   */
  def layer[F[_]: Monad, K, V](top: Cache[F, K, V], bottom: Cache[F, K, V]): Cache[F, K, V] = 
    new LayeredCache[F, K, V](top, bottom)

  private class LayeredCache[F[_]: Monad, K, V](
    topLayer: Cache[F, K, V],
    bottomLayer: Cache[F, K, V]
  ) extends Cache[F, K, V]{
    def lookup(k: K): F[Option[V]] = 
      topLayer.lookup(k).flatMap{
        case s@Some(_) => s.pure[F].widen
        case None => bottomLayer.lookup(k).flatMap{
          case None => Option.empty[V].pure[F]
          case s@Some(v) => 
            topLayer.insert(k, v).as(s).widen
        }
      }
    
    def insert(k: K, v: V): F[Unit] = 
      topLayer.insert(k, v) >>
      bottomLayer.insert(k, v)
    
    def delete(k: K): F[Unit] = 
      topLayer.delete(k) >> 
      bottomLayer.delete(k)
    
  }

  def keySpacePubSubLayered[F[_]: Async](
    topCache: Cache[F, String, String],
    connection: RedisConnection[F],
    namespace: String,
    setOpts: RedisCommands.SetOpts
  ): Resource[F, Cache[F, String, String]] = {
    val nameSpaceStarter = namespace ++ ":"
    RedisPubSub.fromConnection(
      connection,
      4096,
      Function.const(Applicative[F].unit),
      Function.const(Applicative[F].unit)
    ).evalMap{ pubsub => 
      def invalidateTopCache(message: RedisPubSub.PubSubMessage.PMessage): F[Unit] = {
        val channel = message.channel
        val msg = message.message
        val keyR = ("__keyspace.*__:" + nameSpaceStarter + "(.*)").r
        val parsed: String = channel match {
          case keyR(key) => key
        }
        msg match {
          case "set" | "expired" | "del" => topCache.delete(parsed) >> Concurrent[F].unit.map(_ => println(s"Deleted $parsed"))
          case _ => Concurrent[F].unit.map(_ => println(s"Unhandled $message"))
        }
      }
      pubsub.psubscribe(s"__keyspace*__:$nameSpaceStarter*", invalidateTopCache)
        .as(pubsub)
    }.flatMap(pubsub => pubsub.runMessages.background.void).as{
      val redis = instance(connection, namespace, setOpts)
      layer(topCache, redis)
    }
  }

  // Does not require any redis changes
  // Does not see redis expirations so you will want expirations on the top 
  // cache to mirror the cache as best as possible as there will be some 
  // delays here.
  def channelBasedLayered[F[_]: Async](
    topCache: Cache[F, String, String],
    connection: RedisConnection[F],
    namespace: String,
    setOpts: RedisCommands.SetOpts
  ): Resource[F, Cache[F, String, String]] = {
    val channel = namespace
    val redis = instance(connection, namespace, setOpts)
    val layered = layer(topCache, redis)
    def publishChange(key: String): F[Int] = 
      RedisConnection.runRequestTotal[F, Int](cats.data.NonEmptyList.of("publish", channel, key), None).run(connection)
    val cache = new Cache[F, String, String]{
      def lookup(k: String): F[Option[String]] = layered.lookup(k)
      
      def insert(k: String, v: String): F[Unit] = layered.insert(k, v) >> publishChange(k).void
      
      def delete(k: String): F[Unit] = layered.delete(k) >> publishChange(k).void
    }
    RedisPubSub.fromConnection(connection, 4096, Function.const(Applicative[F].unit), Function.const(Applicative[F].unit)).flatMap{
      pubsub => 
        Resource.eval(pubsub.subscribe(channel, {message: RedisPubSub.PubSubMessage.Message => topCache.delete(message.message)})) >>
        pubsub.runMessages.background.as(cache)
    }
  }

  def instance[F[_]: Async](
    connection: RedisConnection[F],
    namespace: String,
    setOpts: RedisCommands.SetOpts
  ): Cache[F, String, String] = new RedisCacheBase[F](connection, namespace, setOpts)

  private class RedisCacheBase[F[_]: Async](
    connection: RedisConnection[F],
    namespace: String,
    setOpts: RedisCommands.SetOpts
  ) extends Cache[F, String, String]{
    private val nameSpaceStarter = namespace ++ ":"

    def lookup(k: String): F[Option[String]] = 
      RedisCommands.get(nameSpaceStarter ++ k).run(connection)
    
    def insert(k: String, v: String): F[Unit] = 
      RedisCommands.set(nameSpaceStarter ++ k, v, setOpts).void.run(connection)
    
    def delete(k: String): F[Unit] = 
      RedisCommands.del(nameSpaceStarter ++ k).void.run(connection)
  }
  
}