package io.chrisdavenport.rediculous.concurrent

import cats.syntax.all._
import cats._
import cats.conversions._
import cats.effect._
import io.chrisdavenport.mules._
import io.chrisdavenport.rediculous._
import cats.effect.syntax.all._
import cats.data.Func
import io.chrisdavenport.singlefibered.SingleFibered

object RedisCache {

  

  /** Layering Function Allows you to put a In-Memory Cache
   *
   * Lookups start at the top layer, and if present don't go further. If absent progresses
   * to the lower cache and will insert into the top layer before the value is returned
   * 
   * inserts and deletes are proliferated to top and then bottom
   */
  def layer[F[_]: Concurrent, K, V](top: Cache[F, K, V], bottom: Cache[F, K, V]): F[Cache[F, K, V]] = {
    val f: K => F[Option[V]] = {(k: K) => bottom.lookup(k).flatMap{
          case None => Option.empty[V].pure[F]
          case s@Some(v) => 
            top.insert(k, v).as(s).widen
        }
      }
    SingleFibered.prepareFunction(f).map(preppedF => 
      new LayeredCache[F, K, V](top, bottom, preppedF)
    )
  }
    

  private class LayeredCache[F[_]: Monad, K, V](
    topLayer: Cache[F, K, V],
    bottomLayer: Cache[F, K, V],
    lookupCached: K => F[Option[V]]
  ) extends Cache[F, K, V]{
    def lookup(k: K): F[Option[V]] = 
      topLayer.lookup(k).flatMap{
        case s@Some(_) => s.pure[F].widen
        case None => lookupCached(k)
      }
      
    
    def insert(k: K, v: V): F[Unit] = 
      topLayer.insert(k, v) >>
      bottomLayer.insert(k, v)
    
    def delete(k: K): F[Unit] = 
      topLayer.delete(k) >> 
      bottomLayer.delete(k)
    
  }

  /**
   * A Keyspace Based Pubsub Layered Cache. 
   * 
   * Redis with the right configuration allows pubsub notifications over
   * all of its internal modifications to a section of the keyspace.
   * 
   * https://redis.io/topics/notifications
   * 
   * Configuring this is as simple locally as 
   * redis-cli config set notify-keyspace-events KA
   * or by modifying your server config.
   * 
   * Notably, in cluster mode these do not leave the local server unlike normal
   * pubsub events so connections must be made to all servers in the cluster.
   * 
   * Updates are not immediate, so should not be seen as atomic, but
   * are very useful for keeping your local caches synced with the 
   * redis store.
   **/
  def keySpacePubSubLayered[F[_]: Async](
    topCache: Cache[F, String, String],
    connection: RedisConnection[F],
    namespace: String,
    setOpts: RedisCommands.SetOpts,
    additionalActionOnDelete: Option[String => F[Unit]] = None
  ): Resource[F, Cache[F, String, String]] = {
    val nameSpaceStarter = namespace ++ ":"
    RedisPubSub.fromConnection(
      connection,
      clusterBroadcast = true
    ).evalMap{ pubsub => 
      def invalidateTopCache(message: RedisPubSub.PubSubMessage.PMessage): F[Unit] = {
        val channel = message.channel
        val msg = message.message
        val keyR = ("__keyspace.*__:" + nameSpaceStarter + "(.*)").r
        val parsed: String = channel match {
          case keyR(key) => key
        }
        msg match {
          case "set" | "expired" | "del" => topCache.delete(parsed)  >> additionalActionOnDelete.traverse_(_.apply(message.message))
          case _ => Concurrent[F].unit
        }
      }
      pubsub.psubscribe(s"__keyspace*__:$nameSpaceStarter*", invalidateTopCache)
        .as(pubsub)
    }.flatMap(pubsub => pubsub.runMessages.background.void).evalMap {_ => 
      val redis = instance(connection, namespace, setOpts)
      layer(topCache, redis)
    }
  }

  /**
   * A Pubsub Channel Based Layered Cache. Other nodes utilizing the same cache notify
   * each other via RedisPubSub.
   * 
   * As a result the only changes represented are those that are represented 
   * by the nodes, modifications based in redis are not seen. Such as redis expirations.
   * A cache with an infinite lifetime in redis will see correct information in their
   * local cache at all times. A cache that sets expiration in redis can be off by the
   * period of retention of their local cache. Assuming you get the data the moment before
   * it expires inside redis.
   * 
   * If you are using expirations, which you should a shorter retention period for
   * your local cache will prevent you from being too out of date.
   * 
   * Example
   * 
   * val r = for {
   *   // maxQueued: How many elements before new submissions semantically block. Tradeoff of memory to queue jobs. 
   *   // Default 1000 is good for small servers. But can easily take 100,000.
   *   // workers: How many threads will process pipelined messages.
   *   connection <- RedisConnection.queued[IO].withHost(host"localhost").withPort(port"6379").withMaxQueued(10000).withWorkers(workers = 1).build
   *   topCache <- Resource.eval(_root_.io.chrisdavenport.mules.MemoryCache.ofSingleImmutableMap[IO, String, String](None))
   *   pubsub <- RedisPubSub.fromConnection(connection)
   *   cache <- RedisCache.channelBasedLayered(topCache, connection, pubsub, "namespace2", RedisCommands.SetOpts(Some(60), None, None, false), {(s: String) => IO.println(s"Deleted: $s")}.some)
   * } yield (connection, cache)
   **/
  def channelBasedLayered[F[_]: Async](
    topCache: Cache[F, String, String],
    connection: RedisConnection[F],
    pubsub: RedisPubSub[F],
    namespace: String,
    setOpts: RedisCommands.SetOpts,
    additionalActionOnDelete: Option[String => F[Unit]] = None
  ): Resource[F, Cache[F, String, String]] = {
    val channel = namespace
    val redis = instance(connection, namespace, setOpts)
    def publishChange(key: String) = RedisCommands.publish(channel, key).run(connection)
    
    Resource.eval(layer[F, String, String](topCache, redis)).flatMap{
      case layered => 
        Resource.eval(pubsub.subscribe(channel, {message: RedisPubSub.PubSubMessage.Message => topCache.delete(message.message) >> additionalActionOnDelete.traverse_(_.apply(message.message))})) >>
        pubsub.runMessages.background.as{
          new Cache[F, String, String]{
            def lookup(k: String): F[Option[String]] = layered.lookup(k)
            
            def insert(k: String, v: String): F[Unit] = layered.insert(k, v) >> publishChange(k).void
            
            def delete(k: String): F[Unit] = layered.delete(k) >> publishChange(k).void
          }
        }
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