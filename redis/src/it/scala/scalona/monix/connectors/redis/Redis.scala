package scalona.monix.connectors.client

import java.util
import java.util.Date

import io.lettuce.core.{ KeyValue, MapScanCursor, RedisClient, RedisFuture, TransactionResult }
import io.lettuce.core.api.StatefulRedisConnection
import monix.reactive.Observable
import monix.eval.{ Task, TaskLike }
import shapeless.ops.union.Keys

import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._

class Redis[K, V](implicit client: StatefulRedisConnection[K, V]) {

  implicit val redisFutureTaskLike = new TaskLike[RedisFuture] {
    def apply[A](rf: RedisFuture[A]): Task[A] =
      Task.fromFuture(rf.asScala)
  }

  val redisClient: RedisClient = RedisClient.create("client://localhost:6379/0")
  val connection: StatefulRedisConnection[String, String] = redisClient.connect()

  def append(key: K, value: V): Task[Long] = {
    Task.from(client.async().append(key, value)).map(_.toLong)
  }

  def bitcount(key: K): Task[Long] = {
    Task.from(client.async().bitcount(key)).map(_.toLong)
  }

  def brpop(source: Seq[K], timeout: Long): Task[KeyValue[K, V]] = {
    Task.from(client.async().brpop(timeout, source: _*).asScala)
  }

  def brpoplpush(source: K, destination: K, timeout: Long): Task[V] = {
    Task.from(client.async().brpoplpush(timeout, source, destination))
  }

  def brpoplpush(source: K, destination: K, timeout: Long, ttl: Long): Task[V] = {
    Task
      .from(client.async().brpoplpush(timeout, source, destination))
      .flatMap(v => Task.from(client.async().ttl(destination)).map(_ => v))
  }

  def bzpopmin(keys: Seq[K], timeout: Long) = {
    Task.from(client.async().bzpopmin(timeout, keys: _*))
  }

  def bzpopmax(keys: Seq[K], timeout: Long) = {
    Task.from(client.async().bzpopmax(timeout, keys: _*))
  }

  def dbsize(key: K): Task[Long] = {
    Task.from(client.async().dbsize()).map(_.longValue)
  }

  def decr(key: K): Task[Long] = {
    Task.from(client.async().decr(key)).map(_.longValue)
  }

  def decrby(key: K, amount: Long): Task[Long] = {
    Task.from(client.async().decrby(key, amount)).map(_.longValue)
  }

  def del(keys: Seq[K]): Task[Long] = {
    Task.from(client.async().del(keys: _*).asScala).map(_.longValue)
  }

  def dump(key: K): Task[Array[Byte]] = {
    Task.from(client.async().dump(key))
  }

  //eval, evalsha
  def exec(key: K): Task[TransactionResult] = {
    Task.from(client.async().exec())
  }

  def exists(key: Seq[K]): Task[Long] = {
    Task.from(client.async().exists(key: _*)).map(_.toLong)
  }

  def expire(key: K, seconds: Long): Task[Boolean] = {
    Task.from(client.async().expire(key, seconds)).map(_.booleanValue())
  }
  //expireat

  def flushall(): Task[String] = {
    Task.from(client.async().flushall())
  }
  def flushdb(): Task[String] = {
    Task.from(client.async().flushdb())
  }
  //geoadd, geohash, geopos, geodist, georadius, georadiusbymemeber

  def getbit(key: K, offset: Long): Task[Long] = {
    Task.from(client.async().getbit(key, offset)).map(_.longValue)
  }

  def get(key: K): Task[V] = {
    Task.from(client.async().get(key))
  }

  def getset(key: K, value: V): Task[V] = {
    Task.from(client.async().getset(key, value))
  }

  def hdel(key: K, fields: Seq[K]): Task[Long] = {
    Task.from(client.async().hdel(key, fields: _*)).map(_.longValue)
  }

  def hexists(key: K, field: K): Task[Boolean] = {
    Task.from(client.async().hexists(key, field)).map(_.booleanValue)
  }

  def hget(key: K, field: K): Task[V] = {
    Task.from(client.async().hget(key, field))
  }

  def hgetall(key: K): Task[util.Map[K, V]] = {
    Task.from(client.async().hgetall(key))
  }

  def hincby(key: K, field: K, amount: Long): Task[Long] = {
    Task.from(client.async().hincrby(key, field, amount)).map(_.longValue)
  }

  def hincbyfloat(key: K, field: K, amount: Float): Task[Long] = {
    Task.from(client.async().hincrbyfloat(key, field, amount)).map(_.longValue)
  }

  def hkeys(key: K): Task[List[K]] = {
    Task.from(client.async().hkeys(key)).map(_.asScala.toList)
  }

  def hlen(key: K): Task[Long] = {
    Task.from(client.async().hlen(key)).map(_.longValue)
  }

  def hmget(key: K, fields: Seq[K]): Observable[KeyValue[K, V]] = {
    Observable.fromReactivePublisher(client.reactive().hmget(key, fields: _*))
  }

  def hmset(key: K, map: Map[K, V]): Task[String] = {
    Task.from(client.async().hmset(key, map.asJava))
  }

  def hset(key: K, field: K, value: V): Task[Boolean] = {
    Task.from(client.async().hset(key, field, value)).map(_.booleanValue)
  }

  //Not in redis api
  def hsetExpire(key: K, field: K, value: V, seconds: Long)(
    implicit client: StatefulRedisConnection[K, V]): Task[Boolean] = {
    Task
      .from(client.async().hset(key, field, value).asScala)
      .flatMap(_ => Task.from(client.async().expire(key, seconds)).map(_.booleanValue))
  }

  def hsetnx(key: K, field: K, value: V): Task[Boolean] = {
    Task.from(client.async().hsetnx(key, field, value)).map(_.booleanValue)
  }

  def hstrlen(key: K, field: K): Task[Long] = {
    Task.from(client.async().hstrlen(key, field)).map(_.longValue)
  }

  def hvals(key: K): Task[List[V]] = {
    Task.from(client.async().hvals(key)).map(_.asScala.toList)
  }

  def incr(key: K): Task[Long] = {
    Task.from(client.async().incr(key)).map(_.longValue)
  }

  def incrby(key: K, amount: Long): Task[Long] = {
    Task.from(client.async().incrby(key, amount)).map(_.longValue)
  }

  def incrbydouble(key: K, amount: Double): Task[Double] = {
    Task.from(client.async().incrbyfloat(key, amount)).map(_.doubleValue)
  }

  def info(section: String): Task[String] = {
    Task.from(client.async().info(section))
  }
  //lolwut

  def keys(pattern: K): Task[List[K]] = {
    Task.from(client.async().keys(pattern)).map(_.asScala.toList)
  }

  def lastave(section: String): Task[Date] = {
    Task.from(client.async().lastsave())
  }

  def lindex(key: K, index: Long): Task[V] = {
    Task.from(client.async().lindex(key, index))
  }

  def linsert(key: K, before: Boolean, pivot: V, value: V): Task[Long] = {
    Task.from(client.async().linsert(key, before, pivot, value)).map(_.longValue)
  }

  def llen(key: K): Task[Long] = {
    Task.from(client.async().llen(key)).map(_.longValue)
  }

  def lpop(key: K): Task[V] = {
    Task.from(client.async().lpop(key))
  }

  def lpush(key: K, values: List[V]): Task[Long] = {
    Task.from(client.async().lpush(key, values: _*)).map(_.longValue)
  }

  def lpushx(key: K, values: List[V]): Task[Long] = {
    Task.from(client.async().lpushx(key, values: _*)).map(_.longValue)
  }

  def lrange(key: K, start: Long, stop: Long): Task[List[V]] = {
    Task.from(client.async().lrange(key, start, stop)).map(_.asScala.toList)
  }

  def lrem(key: K, count: Long, value: V): Task[Long] = {
    Task.from(client.async().lrem(key, count, value)).map(_.longValue)
  }

  def lset(key: K, index: Long, value: V): Task[String] = {
    Task.from(client.async().lset(key, index, value))
  }
  def ltrim(section: String): Task[String] = {
    Task.from(client.async().info(section))
  }
  //memory doctor, memory help, malloc-stats, purge, stats, usage

  def mget(keys: List[K]): Task[List[KeyValue[K, V]]] = {
    Task.from(client.async().mget(keys: _*)).map(_.asScala.toList)
  }

  def migrate(host: String, port: Int, key: K, db: Int, timeout: Long): Task[String] = {
    Task.from(client.async().migrate(host, port, key, db, timeout))
  }
  //module_list, load, unload, monitor

  def move(key: K, db: Int): Task[Boolean] = {
    Task.from(client.async().move(key, db)).map(_.booleanValue)
  }

  def mset(map: Map[K, V]): Task[String] = {
    Task.from(client.async().mset(map.asJava))
  }
  def msetnx(map: Map[K, V]): Task[Boolean] = {
    Task.from(client.async().msetnx(map.asJava)).map(_.booleanValue)
  }
  def multi(section: String): Task[String] = {
    Task.from(client.async().multi())
  }
  //object

  def persist(key: K): Task[Boolean] = {
    Task.from(client.async().persist(key)).map(_.booleanValue)
  }
  def pexpire(key: K, milliseconds: Long): Task[Boolean] = {
    Task.from(client.async().pexpire(key, milliseconds)).map(_.booleanValue)
  }
  def pexpireat(key: K, date: Date): Task[Boolean] = {
    Task.from(client.async().pexpireat(key, date)).map(_.booleanValue)
  }
  def pfadd(key: K, values: List[V]): Task[Long] = {
    Task.from(client.async().pfadd(key, values: _*)).map(_.longValue)
  }
  def pfcount(keys: List[K]): Task[Long] = {
    Task.from(client.async().pfcount(keys: _*)).map(_.longValue)
  }
  def pfmerge(destkey: K, sourcekeys: List[K]): Task[String] = {
    Task.from(client.async().pfmerge(destkey, sourcekeys: _*))
  }
  //ping

  def psetex(key: K, milliseconds: Long, value: V): Task[String] = {
    Task.from(client.async().psetex(key, milliseconds, value))
  }
  //psubscribe, pubsub
  def pttl(key: K): Task[Long] = {
    Task.from(client.async().pttl(key)).map(_.longValue)
  }
  def publish(channel: K, message: V): Task[Long] = {
    Task.from(client.async().publish(channel, message)).map(_.longValue)
  }
  //punsubscribe

  def quit(section: String): Task[String] = {
    Task.from(client.async().quit())
  }
  def randomkey(section: String): Task[V] = {
    Task.from(client.async().randomkey())
  }
  def readonly(): Task[String] = {
    Task.from(client.async().readOnly())
  }
  def readwrite(): Task[String] = {
    Task.from(client.async().readWrite())
  }
  def rename(key: K, newkey: K): Task[String] = {
    Task.from(client.async().rename(key, newkey))
  }
  def renamenx(key: K, newkey: K): Task[Boolean] = {
    Task.from(client.async().renamenx(key, newkey)).map(_.booleanValue)
  }
  def restore(key: K, ttl: Long, value: Array[Byte]): Task[String] = {
    Task.from(client.async().restore(key, ttl, value))
  }
  def role(): Task[List[Any]] = {
    Task.from(client.async().role()).map(_.asScala.toList)
  }

  def rpop(key: K): Task[V] = {
    Task.from(client.async().rpop(key))
  }
  def rpoplpush(source: K, destination: K): Task[V] = {
    Task.from(client.async().rpoplpush(source, destination))
  }

  def rpush(key: K, values: List[V]): Task[Long] = {
    Task.from(client.async().rpush(key, values: _*)).map(_.longValue)
  }

  def rpushx(key: K, values: List[V]): Task[Long] = {
    Task.from(client.async().rpushx(key, values: _*)).map(_.longValue)
  }
  def sadd(key: K, members: List[V]): Task[Long] = {
    Task.from(client.async().sadd(key, members: _*)).map(_.longValue)
  }
  def save(): Task[String] = {
    Task.from(client.async().save())
  }
  def scard(key: K): Task[Long] = {
    Task.from(client.async().scard(key)).map(_.longValue)
  }
  //script debug, exusts, flush, kill, load

  def sdiff(keys: List[K]): Task[Set[V]] = {
    Task.from(client.async().sdiff(keys: _* )).map(_.asScala.toSet)
  }
  def sdiffstore(destination: K, keys: List[K]): Task[Long] = {
    Task.from(client.async().sdiffstore(destination, keys: _*)).map(_.longValue)
  }
  def select(db: Int): Task[String] = {
    Task.from(client.async().select(db))
  }

  //getbit, getRange,
  def set(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def setbit(key: K, offset: Long, value: Int): Task[Long] = {
    Task.from(client.async().setbit(key, offset, value)).map(_.longValue)
  }
  def setex(key: K, seconds: Long, value: V): Task[String] = {
    Task.from(client.async().setex(key, seconds, value))
  }
  def setnx(key: K, value: V): Task[Boolean] = {
    Task.from(client.async().setnx(key, value)).map(_.booleanValue)
  }

  def setrange(key: K, offset: Long, value: V): Task[Long] = {
    Task.from(client.async().setrange(key, offset, value)).map(_.longValue)
  }
  def sinterstore(destination: K, keys: List[K]): Task[Long] = {
    Task.from(client.async().sinterstore(destination, keys: _*)).map(_.longValue)
  }
  def sismember(key: K, member: V): Task[Boolean] = {
    Task.from(client.async().sismember(key, member)).map(_.booleanValue)
  }

  def slaveof(section: String): Task[String] = {
    Task.from(client.async().info(section))
  }
  def replicaof(section: String): Task[String] = {
    Task.from(client.async().info(section))
  }

  //getbit, getRange,
  def slowlog(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def smembers(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def smove(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def sort(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def spop(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def srandmember(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def srem(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }

  def strlen(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  //subscribe
  def sunion(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }

  def sunionstore(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def swapdb(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def sync(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def psync(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def time(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def touch(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def ttl(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def typed(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  //unsubscribe
  def unlink(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }

  def unwatch(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def wait(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def watch(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def zadd(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def zcard(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def zcount(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def zincrby(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def zinterstore(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def zlexcount(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def zpopmax(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def zpopmin(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def zrange(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def zrangebylex(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def zrevrangebylex(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def zrangebyscore(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def zrank(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def zrem(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def zremrangebylex(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def zremrangebytank(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def zremrangebyscore(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def zrevrange(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def zrevrangebtscore(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }
  def zrevrank(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }

  def zscore(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }

  def zunionstore(key: K, value: V): Task[String] = {
    Task.from(client.async().set(key, value))
  }

}
