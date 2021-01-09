package io.chrisdavenport.rediculous.concurrent

import cats.syntax.all._
import cats._
import cats.conversions._
import cats.effect._
import io.chrisdavenport.mules._
import io.chrisdavenport.rediculous._

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

  def instance[F[_]: Concurrent](
    connection: RedisConnection[F],
    namespace: String,
    setOpts: RedisCommands.SetOpts
  ): Cache[F, String, String] = new RedisCacheBase[F](connection, namespace, setOpts)

  private class RedisCacheBase[F[_]: Concurrent](
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