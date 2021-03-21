---
id: redis title: Redis
---

## Introduction

_Redis_ is an open source, in-memory data structure store, used as a database, cache and message broker providing high
availability, scalability and a outstanding performance. It supports data structures such as string, hashes, lists,
sets, sorted sets with range queries, streams and more. It has a defined a set of [commands](https://redis.io/commands)
to inter-operate with, and most of them are also available from the java api.

This connector has been built on top of [lettuce](https://lettuce.io/), the most popular java library for operating with
a _non blocking_ Redis client.

## Dependency

Add the following dependency:

```scala
libraryDependencies += "io.monix" %% "monix-redis" % "0.6.0"
```

## Getting started

Redis provides a wide range of commands to perform a different set of operations, divided into 15 different groups.
Currently, this connector only provides support for the most common used
ones:  ([Keys](https://redis.io/commands#generic), [Hashes](https://redis.io/commands#hash)
, [List](https://redis.io/commands#list), [Server](https://redis.io/commands#server)
, [Sets](https://redis.io/commands#set), [SortedSets](https://redis.io/commands#sorted_set)
and [Strings](https://redis.io/commands#string)). On continuation, let's get started on how to create a redis _
standalone_ and _cluster_ connection , also will show how to use the standard `UTF` and `ByteArray` codecs and how to
create your own one.

## Create a connection

### Standalone

Represents a connection to a standalone redis server, extending the `monix.connect.redis.client.RedisConnection`
interface, which defines the set of methods to create the connection that encodes in _UTF_ and _Array[Byte]_ and also
supports custom _Codecs_.

In order to create the connection, first we would just need a single `monix.connect.redis.client.RedisUri` relative to
the redis standalone server:

```scala
import monix.connect.redis.client.{RedisConnection, RedisUri}

// RedisUri has an overloaded `apply` which also allows host and port to be passed separately
// like RedisUri("localhost", 6379) 
val redisUri = RedisUri("redis://localhost:6379")

// then we create the connection
val redisConn: RedisConnection = RedisConnection.standalone(redisUri)
```

### Cluster

Creating a **cluster** connection is seamlessly to the standalone one, they both end up encoded the same parent
class `RedisConnection`, but for the fact it's creation requires multiple `RedisUri`s that represents the set of redis
servers in the cluster.

```scala
import monix.connect.redis.client.{RedisConnection, RedisUri}

val redisNode1 = RedisUri("my-redis-node-1", 7000)
val redisNode2 = RedisUri("my-redis-node-1", 7001)
val redisNode3 = RedisUri("my-redis-node-1", 7002)

val redisClusterConn: RedisConnection = RedisConnection.cluster(List(redisNode1, redisNode2, redisNode3))
```

## RedisCmd

Once we have created a `RedisConnection`, the next steps are to actually release and use the resources associated with
that connection. In the following example we will use the default encoding format which is to work and represent _Keys_
and _Values_ as `Strings`, persisting them into `UTF` format in Redis.

```scala
import cats.effect.Resource
import monix.connect.redis.client.{RedisCmd, RedisConnection, RedisUri}
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

val redisUri = RedisUri("redis://localhost:6379")

val redisConn: Resource[Task, RedisCmd[String, String]] = RedisConnection.standalone(redisUri).connectUtf

val k1: String = "key1"
val value: String = "a"
val k2: String = "key2"
val values: List[String] = List("b", "c", "d")

// from here on, we can start using the connection
// as we can appreciate, since `RedisCmd` is a case class,
// we can apply pattern matching against it, which will 
// nicely allow us to de-compose the different RedisCommands its different api.
// alternatively you can also do:  redisConn.use { redisCmd => redisCmd.string.get("k1") }
redisConn.use { case RedisCmd(hash, keys, list, server, set, sortedSet, string) =>
  for {
    _ <- server.flushAll()
    _ <- keys.touch(k1)
    _ <- string.set(k1, value)
    _ <- keys.rename(k1, k2)
    _ <- list.lPush(k1, values: _*)
    v <- string.get(k2)
    _ <- v match {
      case Some(value) => list.lPush(k1, value)
      case None => Task.unit
    }
    _ <- keys.del(k2)
    len <- list.lLen(k1)
  } yield (len)
}.runToFuture
```

## Codecs

In the previous sections we learned how to create a connection to redis and to start using the `RedisCmd` with its
different redis modules. The connection that we created was a resource that provided a
`RedisCmd[String, String]` was expecting `Strings` for both _Keys_ and _Values_. In order to decide how do we want our
redis connection to encode and decode k and v, we would need to pass a custom `Codec` both for key and value. A `Codec`
is a sealed trait conformed by `UTFCodec` and `ByteArrayCodec`, in which you can create instances of those from its
companion object with the respective signatures `utf` and `byteArray`, see below snippet:

```scala
package monix.connect.redis.client

object Codec {
  def utf[T](encoder: T => String, decoder: String => T) = ???

  def byteArray[T](encoder: T => Array[Byte], decoder: Array[Byte] => T) = ???
}
```

You will find some already predefined `Codec` for `Int`, `Float`, `Double`, `BigInt` and `BigDecimal` under the package
object `monix.connect.redis._`.

Still let's show an example of creating a custom codec that mixes `UTFCodec` and `ByteArrayCodec`:

### UTFCodec

In this example we will create two custom `UTFCodec[T]`, one for keys as `Int`
and the other for `Double` which will represent the redis values, resulting in `RedisCmd[Int, Double]`.

These two will be passed as parameters when connecting to redis with *connectUtf*.

```scala
import monix.connect.redis.client.{Codec, RedisCmd, RedisConnection, RedisUri, UtfCodec}
import monix.execution.CancelableFuture
import monix.execution.Scheduler.Implicits.global

import scala.util.{Failure, Try}

// there is already a predefined int utf codec under `monix.connect.redis._`
implicit val intUtfCodec: UtfCodec[Int] = Codec.utf(_.toString, //serializes int to str
  //deserializes str back to int
  str => Try(str.toInt)
    .failed.flatMap { ex =>
    logger.info("Failed to deserialize from Redis to `Int`")
    Failure(ex)
  }.getOrElse(0)
)

// there is already a predefined double utf codec under `monix.connect.redis._`
implicit val doubleUtfCodec: UtfCodec[Double] = Codec.utf(_.toString, //serializes double to str
  //deserializes str back to double
  str => Try(str.toDouble)
    .failed.flatMap { ex =>
    logger.info("Failed to deserialize from Redis to `Double`")
    Failure(ex)
  }.getOrElse(0.0)
)

val redisUri = RedisUri("redis://localhost:6379")

val f: CancelableFuture[Option[Double]] = 
 RedisConnection.standalone(redisUri)
  .connectUtf(intUtfCodec, doubleUtfCodec) //this can be passed implicitly but is explicit for didactic purposes
  .use { redisCmd: RedisCmd[Int, Double] =>
    //your business logic here
    redisCmd.list.lPush(11, 123.134) >> redisCmd.list.rPop(11) //Some(123.134) 
  }.runToFuture
````


### BytesCodec

On the other hand, we could also create a `BytesCodec[T]` to serialize and deserialize to/from `Array[Byte]`.
In this case we will show an example of using `Protobuf` serialization format to dela with redis keys and values:

In below snippet we defined our _proto_ objects, in which `PersonPK` will represent the redis keys and `Person` the values.  

```proto
syntax = "proto3";

package monix.connect.redis.test;

message PersonPk {
    string id = 1;
}

message Person {
    string name = 1;
    int64 age = 2;
    repeated string hobbies = 3;
}
```

Once the scala sources have been generated, we will proceed to creating a `BytesCodec[PersonPk]` and `BytesCodec[Person]`:

```scala
import monix.connect.redis.client.{BytesCodec, Codec}
implicit val personPkCodec: BytesCodec[PersonPk] = 
  Codec.byteArray(pk => PersonPk.toByteArray(pk), bytes => PersonPk.parseFrom(bytes))
implicit val personCodec: BytesCodec[Person] =
  Codec.byteArray(person => Person.toByteArray(person), bytes => Person.parseFrom(bytes))
```

Finally, we are ready to start to create the connection using the previously defined protobuf codecs:

```scala
import monix.connect.redis.client.{Codec, RedisCmd, RedisConnection, RedisUri, UtfCodec}
import monix.connect.redis.test.protobuf.{Person, PersonPk}
import monix.execution.CancelableFuture
import monix.execution.Scheduler.Implicits.global

val redisUri = RedisUri("redis://localhost:6379")

val personPk = PersonPk("personId123")
val hobbies = List("Snowboarding", "Programming")
val person = Person("Alice", 25, hobbies)

val f: CancelableFuture[Option[Person]] =
  RedisConnection.standalone(redisUri)
    .connectUtf(personPkCodec, personCodec)
    .use{ redisCmd: RedisCmd[PersonPk, Person] =>
      for {
        _ <-redisCmd.string.set(personPk, person)
        person <- redisCmd.string.get(personPk)
      } yield person
    }.runToFuture
```

## __Keys__

The below snippet shows a simple example of using key commands.

```scala
import monix.connect.redis.client.{RedisConnection, RedisUri}
import scala.concurrent.duration._

val k: String // assuming that the key already exists
val redisUri = RedisUri("redis://localhost:6379")

RedisConnection.standalone(redisUri)
  .connectUtf
  .use(cmd =>
    for {
      randomKey <- cmd.key.randomKey() //returns a random key from the db
      _ <- cmd.key.expire(k, 100 seconds) //specifies an expiration timeout for the k1
      ttl <- cmd.key.ttl(k) //returns the time to live as `FiniteDuration`
    } yield (randomKey, ttl)
  )
```

## __Hashes__

The following example uses the redis hash api `RedisHash` to insert a single element into a hash and read it back from
the hash.

```scala
import monix.connect.redis.client.{RedisConnection, RedisUri, RedisCmd}
import scala.concurrent.duration._

val key: String 
val field: String 
val value: String 
val redisUri = RedisUri("redis://localhost:6379")
val prefix = "dummy-prefix-"

RedisConnection.standalone(redisUri)
  .connectUtf
  .use { cmd =>
    for {
      _ <- cmd.hash.hSet(key, field, value)
      //adds a prefix to all values in a hash 
      _ <- cmd.hash.hGetAll(key).mapEval { case (f, v) => cmd.hash.hSet(key, f, prefix + v) }.completedL
      prefixedValue <- cmd.hash.hGet(key, field)
    } yield prefixedValue
  }.runToFuture
```

## __Lists__

The following example uses the redis list api `RedisList` to insert elements into a redis list and reading them back
with limited size.

```scala
import monix.connect.redis.client.{RedisCmd, RedisConnection, RedisUri}
import monix.eval.Task

import scala.concurrent.duration._

val key1: String
val key2: String
val values: List[String]
val redisUri = RedisUri("redis://localhost:6379")
val prefix = "dummy-prefix-"

RedisConnection.standalone(redisUri)
  .connectUtf
  .use { cmd =>
    for {
      initialSize <- cmd.list.lPush(key1, values)
      //copies all values from `key1` to `key2` adding a static prefix to each element 
      _ <- cmd.list.lGetAll(key1)
        .mapEval(v => cmd.list.lPush(key2, prefix + v)).completedL
      //checks if key1 and key2 have the same size
      haveSameSize <- cmd.list.lLen(key1).map(_ == initialSize)
    } yield haveSameSize
  }.runToFuture
```

## __Server__

The following code shows how to remove all keys from all dbs in redis using the server api `RedisServer` a very basic
but also common use case:


```scala
import monix.connect.redis.client.{RedisCmd, RedisConnection, RedisUri}
import monix.eval.Task

val redisUri = RedisUri("redis://localhost:6379")
val prefix = "dummy-prefix-"

val f = RedisConnection.standalone(redisUri)
  .connectUtf.use(_.server.flushAll).runToFuture
```

## __Sets__

The [Redis Set commands api](https://redis.io/commands#set) provides operations to work with _sets_, 
see a practical example in below code snippet.

```scala
import monix.connect.redis.client.{RedisCmd, RedisConnection, RedisUri}
import monix.eval.Task
val k1: String
val k2: String
val redisUri = RedisUri("redis://localhost:6379")

val f = RedisConnection.standalone(redisUri)
  .connectUtf
  .use { cmd =>
    for {
      _ <- cmd.set.sAdd(k1, "a", "b", "c") *>
        cmd.set.sAdd(k2, "c", "d") 
      finalSize <- cmd.set.sUnionStore(k1, k2)
    } yield finalSize //4 = ["a", "b", "c", "d"]
  }.runToFuture
```

## __SortedSets__

The [Redis SortedSet commands api](https://redis.io/commands#sorted_set) provides operations to work with _sorted sets_,
see a practical example in below code snippet, where three scored elements (akka `VScore`), are inserted into a sorted set and 
then incrementing the score of the middle one.


```scala
import monix.connect.redis.client.{RedisCmd, RedisConnection, RedisUri}
import monix.connect.redis.domain.{VScore, ZRange}

val k: String
val redisUri: RedisUri

val f = RedisConnection.standalone(redisUri)
  .connectUtf
  .use { cmd =>
    for {
      _ <- cmd.sortedSet.zAdd(k, VScore("Bob", 1)) >> 
        cmd.sortedSet.zAdd(k, VScore("Alice", 2)) >> 
        cmd.sortedSet.zAdd(k, VScore("Jamie", 5))
      //increments middle one by `increment` so it becomes the highest score of the set
      _ <- cmd.sortedSet.zIncrBy(k, 6, "Bob") 
      //returns those members with score higher than 4 ["Bob", "Jamie"]
      zRange <- cmd.sortedSet.zRangeByScore(k, ZRange.gt(5)).toListL
      min <- cmd.sortedSet.zPopMin(k) // Alice
      max <- cmd.sortedSet.zPopMax(k) // Bob
    } yield (min, max, zRange)
  }.runToFuture
```

## __Strings__

The [Redis Strings commands api](https://redis.io/commands#string) provides operations to work with _strings_,
see a practical example in below code snippet, where we insert a string into the given key and get its size.

```scala
import monix.connect.redis.client.{RedisCmd, RedisConnection, RedisUri}

val k: String
val v: String
val redisUri: RedisUri

val f = RedisConnection.standalone(redisUri)
  .connectUtf
  .use { cmd =>
    for {
      _ <- cmd.string.set(k, v)
      size <- cmd.string.strLen(k)
    } yield size
  }.runToFuture
```


## Local testing

The local tests will use the [redis docker image](https://hub.docker.com/_/redis/) from _docker hub_.

### Standalone server

Add the following service description to your `docker-compose.yml` file:

```yaml
 redis:
   image: redis
   ports:
     - 6379:6379
```

Run the following command to build and start the redis server:

```shell script
docker-compose -f ./docker-compose.yml up -d redis
``` 

Check out that the service has started correctly.

Finally, following code shows how you can create the redis connection to the local server, but you would have to modify
that to fit your use case - i.e it will be different to connect to a redis cluster or if authenticating to the server is
needed using with key and secret, etc.)

```scala
import monix.connect.redis.client
import monix.connect.redis.client.{RedisConnection, RedisUri}

val redisUri = RedisUri("redis://host:port")
val standaloneConn = RedisConnection.standalone(redisUri)
``` 

Now you are ready to run your application!

### Cluster 

On the other hand, if you want to test how your application will behave running with a redis cluster, you can use [grokzen/redis-cluster](https://hub.docker.com/r/grokzen/redis-cluster/)

```yaml
  redisCluster:
    restart: always
    image: grokzen/redis-cluster:6.0.5
    ports:
      - "7000:7000"
      - "7001:7001"
      - "7002:7002"
      - "7003:7003"
      - "7004:7004"
      - "7005:7005"
    environment:
      - STANDALONE=true
      - IP=0.0.0.0
```

And then from the application side you would do:

```scala
import monix.connect.redis.client
import monix.connect.redis.client.{RedisConnection, RedisUri}

val redisUris: Seq[RedisUri] = (0 to 5).map(n => RedisUri(s"redis://localhost:${(700 + n)}"))
val clusterConn = RedisConnection.cluster(redisUris)
``` 

## Yet to come

- _Master Replica_ connection.
- _Pub/sub_, _Streams_, _Transactions_, _HyperLogLog_, _Geolocation_, _Scripting_ commands.
