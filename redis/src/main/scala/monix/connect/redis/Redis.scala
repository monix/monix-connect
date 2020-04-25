/*
 * Copyright (c) 2014-2020 by The Monix Project Developers.
 * See the project homepage at: https://monix.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package monix.connect.redis

import java.util.Date

import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.{KeyValue, RedisFuture, ScoredValue, TransactionResult}
import monix.eval.{Task, TaskLike}
import monix.reactive.Observable

import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._
import concurrent.Future

object Redis {

  implicit val redisFutureTaskLike = new TaskLike[RedisFuture] {
    def apply[A](rf: RedisFuture[A]): Task[A] =
      Task.fromFuture(rf.asScala)
  }

  def append[K, V](key: K, value: V)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().append(key, value)).map(_.toLong)

  def bitcount[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().bitcount(key)).map(_.toLong)

  def brpop[K, V](source: Seq[K], timeout: Long)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[KeyValue[K, V]] =
    Task.from(connection.async().brpop(timeout, source: _*).asScala)

  def brpoplpush[K, V](source: K, destination: K, timeout: Long)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[V] =
    Task.from(connection.async().brpoplpush(timeout, source, destination))

  def brpoplpush[K, V](source: K, destination: K, timeout: Long, ttl: Long)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[V] =
    Task
      .from(connection.async().brpoplpush(timeout, source, destination))
      .flatMap(v => Task.from(connection.async().ttl(destination)).map(_ => v))

  def bzpopmin[K, V](keys: Seq[K], timeout: Long)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[KeyValue[K, ScoredValue[V]]] =
    Task.from(connection.async().bzpopmin(timeout, keys: _*))

  def bzpopmax[K, V](keys: Seq[K], timeout: Long)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[KeyValue[K, ScoredValue[V]]] =
    Task.from(connection.async().bzpopmax(timeout, keys: _*))

  def dbsize[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().dbsize()).map(_.longValue)

  def decr[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().decr(key)).map(_.longValue)

  def decrby[K, V](key: K, amount: Long)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().decrby(key, amount)).map(_.longValue)

  def del[K, V](keys: Seq[K])(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().del(keys: _*).asScala).map(_.longValue)

  def dump[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Array[Byte]] =
    Task.from(connection.async().dump(key))

  //eval, evalsha
  def exec[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[TransactionResult] =
    Task.from(connection.async().exec())

  def exists[K, V](key: Seq[K])(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().exists(key: _*)).map(_.toLong)

  def expire[K, V](key: K, seconds: Long)(implicit connection: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task.from(connection.async().expire(key, seconds)).map(_.booleanValue())

  def flushall[K, V]()(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.async().flushall())

  def flushdb[K, V]()(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.async().flushdb())

  def getbit[K, V](key: K, offset: Long)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().getbit(key, offset)).map(_.longValue)

  def get[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[V] =
    Task.from(connection.async().get(key))

  def getset[K, V](key: K, value: V)(implicit connection: StatefulRedisConnection[K, V]): Task[V] =
    Task.from(connection.async().getset(key, value))

  def hdel[K, V](key: K, fields: Seq[K])(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().hdel(key, fields: _*)).map(_.longValue)

  def hexists[K, V](key: K, field: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task.from(connection.async().hexists(key, field)).map(_.booleanValue)

  def hget[K, V](key: K, field: K)(implicit connection: StatefulRedisConnection[K, V]): Task[V] =
    Task.from(connection.async().hget(key, field))

  def hgetall[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Map[K, V]] =
    Task.from(connection.async().hgetall(key)).map(_.asScala.toMap)

  def hincrby[K, V](key: K, field: K, amount: Long)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().hincrby(key, field, amount)).map(_.longValue)

  def hincbyfloat[K, V](key: K, field: K, amount: Float)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().hincrbyfloat(key, field, amount)).map(_.longValue)

  def hkeys[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[List[K]] =
    Task.from(connection.async().hkeys(key)).map(_.asScala.toList)

  def hlen[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().hlen(key)).map(_.longValue)

  def hmget[K, V](key: K, fields: K*)(implicit connection: StatefulRedisConnection[K, V]): Observable[KeyValue[K, V]] =
    Observable.fromReactivePublisher(connection.reactive().hmget(key, fields: _*))

  def hmset[K, V](key: K, map: Map[K, V])(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.async().hmset(key, map.asJava))

  def hset[K, V](key: K, field: K, value: V)(implicit connection: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task.fromFuture(connection.async().hset(key, field, value).asScala).map(_.booleanValue)

  //Not in redis api
  def hsetExpire[K, V](key: K, field: K, value: V, seconds: Long)(
    implicit
    client: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task
      .from(client.async().hset(key, field, value).asScala)
      .flatMap(_ => Task.from(client.async().expire(key, seconds)).map(_.booleanValue))

  def hsetnx[K, V](key: K, field: K, value: V)(implicit connection: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task.from(connection.async().hsetnx(key, field, value)).map(_.booleanValue)

  def hstrlen[K, V](key: K, field: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().hstrlen(key, field)).map(_.longValue)

  def hvals[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[List[V]] =
    Task.from(connection.async().hvals(key)).map(_.asScala.toList)

  def incr[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().incr(key)).map(_.longValue)

  def incrby[K, V](key: K, amount: Long)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().incrby(key, amount)).map(_.longValue)

  def incrbydouble[K, V](key: K, amount: Double)(implicit connection: StatefulRedisConnection[K, V]): Task[Double] =
    Task.from(connection.async().incrbyfloat(key, amount)).map(_.doubleValue)

  def info[K, V](section: String)(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.async().info(section))

  def keys[K, V](pattern: K)(implicit connection: StatefulRedisConnection[K, V]): Task[List[K]] =
    Task.from(connection.async().keys(pattern)).map(_.asScala.toList)

  def lastave[K, V](section: String)(implicit connection: StatefulRedisConnection[K, V]): Task[Date] =
    Task.from(connection.async().lastsave())

  def lindex[K, V](key: K, index: Long)(implicit connection: StatefulRedisConnection[K, V]): Task[V] =
    Task.from(connection.async().lindex(key, index))

  def linsert[K, V](key: K, before: Boolean, pivot: V, value: V)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().linsert(key, before, pivot, value)).map(_.longValue)

  def llen[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().llen(key)).map(_.longValue)

  def lpop[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[V] =
    Task.from(connection.async().lpop(key))

  def lpush[K, V](key: K, values: List[V])(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().lpush(key, values: _*)).map(_.longValue)

  def lpushx[K, V](key: K, values: List[V])(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().lpushx(key, values: _*)).map(_.longValue)

  def lrange[K, V](key: K, start: Long, stop: Long)(implicit connection: StatefulRedisConnection[K, V]): Task[List[V]] =
    Task.from(connection.async().lrange(key, start, stop)).map(_.asScala.toList)

  def lrem[K, V](key: K, count: Long, value: V)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().lrem(key, count, value)).map(_.longValue)

  def lset[K, V](key: K, index: Long, value: V)(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.async().lset(key, index, value))

  def ltrim[K, V](section: String)(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.async().info(section))

  def mget[K, V](keys: List[K])(implicit connection: StatefulRedisConnection[K, V]): Task[List[KeyValue[K, V]]] =
    Task.from(connection.async().mget(keys: _*)).map(_.asScala.toList)

  def migrate[K, V](host: String, port: Int, key: K, db: Int, timeout: Long)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.async().migrate(host, port, key, db, timeout))

  def move[K, V](key: K, db: Int)(implicit connection: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task.from(connection.async().move(key, db)).map(_.booleanValue)

  def mset[K, V](map: Map[K, V])(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.async().mset(map.asJava))

  def msetnx[K, V](map: Map[K, V])(implicit connection: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task.from(connection.async().msetnx(map.asJava)).map(_.booleanValue)

  def multi[K, V](section: String)(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.async().multi())

  def persist[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task.from(connection.async().persist(key)).map(_.booleanValue)

  def pexpire[K, V](key: K, milliseconds: Long)(implicit connection: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task.from(connection.async().pexpire(key, milliseconds)).map(_.booleanValue)

  def pexpireat[K, V](key: K, date: Date)(implicit connection: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task.from(connection.async().pexpireat(key, date)).map(_.booleanValue)

  def pfadd[K, V](key: K, values: List[V])(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().pfadd(key, values: _*)).map(_.longValue)

  def pfcount[K, V](keys: List[K])(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().pfcount(keys: _*)).map(_.longValue)

  def pfmerge[K, V](destkey: K, sourcekeys: List[K])(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.async().pfmerge(destkey, sourcekeys: _*))

  def psetex[K, V](key: K, milliseconds: Long, value: V)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.async().psetex(key, milliseconds, value))

  def pttl[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().pttl(key)).map(_.longValue)

  def publish[K, V](channel: K, message: V)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().publish(channel, message)).map(_.longValue)

  def quit[K, V](section: String)(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.async().quit())

  def randomkey[K, V](section: String)(implicit connection: StatefulRedisConnection[K, V]): Task[V] =
    Task.from(connection.async().randomkey())

  def readonly[K, V]()(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.async().readOnly())

  def readwrite[K, V]()(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.async().readWrite())

  def rename[K, V](key: K, newkey: K)(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.async().rename(key, newkey))

  def renamenx[K, V](key: K, newkey: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task.from(connection.async().renamenx(key, newkey)).map(_.booleanValue)

  def restore[K, V](key: K, ttl: Long, value: Array[Byte])(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.async().restore(key, ttl, value))

  def role[K, V]()(implicit connection: StatefulRedisConnection[K, V]): Task[List[Any]] =
    Task.from(connection.async().role()).map(_.asScala.toList)

  def rpop[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[V] =
    Task.from(connection.async().rpop(key))

  def rpoplpush[K, V](source: K, destination: K)(implicit connection: StatefulRedisConnection[K, V]): Task[V] =
    Task.from(connection.async().rpoplpush(source, destination))

  def rpush[K, V](key: K, values: List[V])(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().rpush(key, values: _*)).map(_.longValue)

  def rpushx[K, V](key: K, values: List[V])(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().rpushx(key, values: _*)).map(_.longValue)

  def sadd[K, V](key: K, members: List[V])(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().sadd(key, members: _*)).map(_.longValue)

  def save[K, V]()(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.async().save())

  def scard[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().scard(key)).map(_.longValue)

  def sdiff[K, V](keys: List[K])(implicit connection: StatefulRedisConnection[K, V]): Task[Set[V]] =
    Task.from(connection.async().sdiff(keys: _*)).map(_.asScala.toSet)

  def sdiffstore[K, V](destination: K, keys: List[K])(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().sdiffstore(destination, keys: _*)).map(_.longValue)

  def set[K, V](key: K, value: V)(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.async().set(key, value))

  def setbit[K, V](key: K, offset: Long, value: Int)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().setbit(key, offset, value)).map(_.longValue)

  def setex[K, V](key: K, seconds: Long, value: V)(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.async().setex(key, seconds, value))

  def setnx[K, V](key: K, value: V)(implicit connection: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task.from(connection.async().setnx(key, value)).map(_.booleanValue)

  def setrange[K, V](key: K, offset: Long, value: V)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().setrange(key, offset, value)).map(_.longValue)

  def sinterstore[K, V](destination: K, keys: List[K])(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().sinterstore(destination, keys: _*)).map(_.longValue)

  def sismember[K, V](key: K, member: V)(implicit connection: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task.from(connection.async().sismember(key, member)).map(_.booleanValue)

  def slaveof[K, V](host: String, port: Int)(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.async().slaveof(host, port))

  def slowlogGet[K, V]()(implicit connection: StatefulRedisConnection[K, V]): Task[List[Any]] = //todo revise
    Task.from(connection.async().slowlogGet()).map(_.asScala.toList)

  def smembers[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Set[V]] =
    Task.from(connection.async().smembers(key)).map(_.asScala.toSet)

  def smove[K, V](source: K, destination: K, member: V)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task.from(connection.async().smove(source, destination, member)).map(_.booleanValue)

  def sort[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[List[V]] =
    Task.from(connection.async().sort(key)).map(_.asScala.toList)

  def spop[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[V] =
    Task.from(connection.async().spop(key))

  def spop[K, V](key: K, count: Long)(implicit connection: StatefulRedisConnection[K, V]): Task[Set[V]] =
    Task.from(connection.async().spop(key, count)).map(_.asScala.toSet)

  def srandmember[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[V] =
    Task.from(connection.async().srandmember(key))

  def srem[K, V](key: K, members: List[V])(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().srem(key, members: _*)).map(_.longValue)

  def strlen[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().strlen(key)).map(_.longValue)

  def sunion[K, V](keys: List[K])(implicit connection: StatefulRedisConnection[K, V]): Task[Set[V]] =
    Task.from(connection.async().sunion(keys: _*)).map(_.asScala.toSet)

  def sunionstore[K, V](destination: K, keys: List[K])(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().sunionstore(destination, keys: _*)).map(_.longValue)

  def swapdb[K, V](db1: Int, db2: Int)(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.async().swapdb(db1, db2))

  def time[K, V]()(implicit connection: StatefulRedisConnection[K, V]): Task[List[V]] =
    Task.from(connection.async().time()).map(_.asScala.toList)

  def touch[K, V](keys: List[K])(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().touch(keys: _*)).map(_.longValue)

  def ttl[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().ttl(key)).map(_.longValue)

  def unlink[K, V](keys: List[K])(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().unlink(keys: _*)).map(_.longValue)

  def unwatch[K, V]()(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.async().unwatch())

  def watch[K, V](keys: List[K])(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.async().watch(keys: _*))

  def zadd[K, V](key: K, score: Double, member: V)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] = //todo add alternatives
    Task.from(connection.async().zadd(key, score, member)).map(_.longValue)

  def zcard[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().zcard(key)).map(_.longValue)

  def zincrby[K, V](key: K, amount: Double, value: V)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[Double] =
    Task.from(connection.async().zincrby(key, amount, value)).map(_.doubleValue)

  def zinterstore[K, V](destination: K, keys: List[K])(implicit connection: StatefulRedisConnection[K, V]): Task[Long] = //add alternatives
    Task.from(connection.async().zinterstore(destination, keys: _*)).map(_.longValue)

  def zpopmax[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[ScoredValue[V]] =
    Task.from(connection.async().zpopmax(key))

  def zpopmax[K, V](key: K, count: Long)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[List[ScoredValue[V]]] =
    Task.from(connection.async().zpopmax(key, count)).map(_.asScala.toList)

  def zpopmin[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[ScoredValue[V]] =
    Task.from(connection.async().zpopmin(key))

  def zpopmin[K, V](key: K, count: Long)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[List[ScoredValue[V]]] =
    Task.from(connection.async().zpopmin(key, count)).map(_.asScala.toList)

  def zrange[K, V](key: K, start: Long, stop: Long)(implicit connection: StatefulRedisConnection[K, V]): Task[List[V]] =
    Task.from(connection.async().zrange(key, start, stop)).map(_.asScala.toList)

  def zrank[K, V](key: K, value: V)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().zrank(key, value)).map(_.longValue)

  def zrem[K, V](key: K, members: List[V])(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().zrem(key, members: _*)).map(_.longValue)

  def zrevrank[K, V](key: K, value: V)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().zrevrank(key, value)).map(_.longValue)

  def zscore[K, V](key: K, value: V)(implicit connection: StatefulRedisConnection[K, V]): Task[Double] =
    Task.from(connection.async().zscore(key, value)).map(_.doubleValue)

  def zunionstore[K, V](key: K, members: List[K])(implicit connection: StatefulRedisConnection[K, V]): Task[Long] = //pending alternatives
    Task.from(connection.async().zunionstore(key, members: _*)).map(_.toLong)

}
