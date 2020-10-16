---
id: redis
title: Redis
---

## Introduction
_Redis_ is an open source, in-memory data structure store, used as a database, cache and message broker providing high availability, scalability and a outstanding performance. 
It supports data structures such as string, hashes, lists, sets, sorted sets with range queries, streams and more.
It has a defined a set of [commands](https://redis.io/commands) to inter-operate with, and most of them are also available from the java api.

This connector has been built on top of [lettuce](https://lettuce.io/), the most popular java library for operating with a _non blocking_ Redis client.

Then `monix-redis` is nothing els that a nice interoperability between the reactive types returned by the lettuce api like (`Mono<T>` and `Flux<T>`) from  [Reactor](https://projectreactor.io/docs/core/release/reference/) project or `RedisFuture[T]`
At the same time that it returns the right values from scala lang and not form java, resulting in a idiomatic api that greatly reduces boilerplate code makes the user to have a nice experience while integrating 
 redis operations using monix.
 
 See an example in below table: 
 
  | _Async_ | _Reactive_ | _Monix_ |
  | :---: | :---: | :---: |
  | _RedisFuture<java.lang.Long>_ | _Mono<java.lang.Long>_ | _Task[Long]_  |
  | _RedisFuture<java.lang.Boolean>_ | _Mono<java.lang.Boolean>_ | _Task[Boolean]_ |
  | _RedisFuture<java.utli.List<V>>_ | _Flux<V>_ | _Observable[V]_ |
  | ... | ... | ... |
  
  
## Dependency

Add the following dependency:

```scala
libraryDependencies += "io.monix" %% "monix-redis" % "0.5.0"
```

## Getting started

Redis provides a wide range of commands to perform a different range of operations, divided into 15 different groups. 
Currently, this connector only  provides support for the most common used ones:  ([Keys](https://redis.io/commands#generic), [Hashes](https://redis.io/commands#hash), [List](https://redis.io/commands#list), [Pub/Sub](https://redis.io/commands#pubsub), [Server](https://redis.io/commands#server), [Sets](https://redis.io/commands#set), [SortedSets](https://redis.io/commands#sorted_set), [Streams](https://redis.io/commands#stream) and [Strings](https://redis.io/commands#string)).
Each of these modules has its own object located under `monix.connect.redis` package, being `Redis` the one that aggregates them all. But they can be individually used too.

Apart of that, you will only need to define an implicit instance of `StatefulRedisConnection[K, V]` in the scope of the . 
 
On continuation let's show an example for each of the redis data group:
 
### __Keys__
 
The following example uses the redis keys api `RedisKey` to show a little example on working with some basic key operations.

```scala
import monix.connect.redis.RedisKey

//given two keys and a value
val key1: K // assuming that k1 initially exists
val key2: K // k2 does not exists
val value: String

//when
val t: Task[Long, Boolean, Long, Long, Long, Long] = {
  for {
    initialTtl <- RedisKey.ttl(key1)          //checks the ttl when it hasn't been set yet
    expire <-  RedisKey.expire(key1, 5)       //sets the ttl to 5 seconds
    finalTtl <- RedisKey.ttl(key1)            //checks the ttl again
    existsWithinTtl <- RedisKey.exists(key1)  //checks whether k1 exists or not
    _ <- RedisKey.rename(key1, key2)          //renames k1 to k2
    existsRenamed <- RedisKey.exists(key2)    //checks that it exists after being renamed
    _ <- Task.sleep(6.seconds)
    existsAfterFiveSeconds <- RedisKey.exists(key2) //after 6 seconds checks ttl again
  } yield (initialTtl, expire, finalTtl, existsWithinTtl, existsRenamed, existsAfterFiveSeconds)
}

//then
val (initialTtl, expire, finalTtl, existsWithinTtl, existsRenamed, existsAfterFiveSeconds) = t.runSyncUnsafe()
initialTtl should be < 0L
finalTtl should be > 0L
expire shouldBe true
existsWithinTtl shouldBe 1L
existsRenamed shouldBe 1L
existsAfterFiveSeconds shouldBe 0L
```

### __Hashes__

The following example uses the redis hash api `RedisHash` to insert a single element into a hash and read it back from the hash.

```scala
import monix.connect.redis.RedisHash

val key: String = ???
val field: String = ???
val value: String = ???
 
val t: Task[String] = for {
    _ <- RedisHash.hset(key, field, value)
    v <- RedisHash.hget(key, field)
} yield v
   
val fv: Future[String] = t.runToFuture()
```

### __Lists__

The following example uses the redis list api `RedisList` to insert elements into a redis list and reading them back with limited size.

```scala
import monix.connect.redis.RedisList

val key: String = String
val values: List[String]
  
val tl: Task[List[String]] = for {
  _ <- RedisList.lpush(key, values: _*)
  l <- RedisList.lrange(key, 0, values.size).toListL
} yield l
  
//a safer alternative to use that will return Observable[v] rather than Task[List[V]]
val ob: Observable[String] = for {
  _ <- Observable.fromTask(RedisList.lpush(key, values: _*))
  ob <- RedisList.lrange(key, 0, values.size)
} yield ob
```

### __Pub/Sub__

Coming soon.

### __Server__

The following code shows how to remove all keys from all dbs in redis using the server api `RedisServer` a very basic but also common use case: 

```scala
import monix.connect.redis.RedisServer
 
val t: Task[String] = RedisServer.flushall() //returns a simple string reply
```

### __Sets__
 
The following code sample uses the redis sets api from `RedisSet` object, this one is a bit longer than the others but not more complex.

```scala
import monix.connect.redis.RedisSet

//given three keys and two redis set of values
val k1: K 
val m1: Set[String]
val k2: K 
val m2: Set[String]
val k3: K

//when
val f1: Future[(Long, Long, Boolean)] = {
  for {
    size1 <- RedisSet.sadd(k1, m1: _*) //first list is added to the first hey
    size2 <- RedisSet.sadd(k2, m2: _*) //second list is added to second first key
    _     <- RedisSet.sadd(k3, m1: _*) //first list is added to the third key
    moved <- RedisSet.smove(k1, k2, m1.head) //moves the head member from the first set to the second one 
  } yield { (size1, size2, moved) }
}.runToFuture() //this is not safe and only

//and
val f2: Task[(Long, Long, List[String], List[String])] = {
  for {
    s1    <- RedisSet.smembers(k1).toListL //get members form k1
    s2    <- RedisSet.smembers(k2).toListL //get members form k2
    union <- RedisSet.sunion(k1, k2).toListL //get members form k2 and k2
    diff  <- RedisSet.sdiff(k3, k1).toListL //get the diff members between k1 and k2
  } yield (s1, s2, union, diff)
}

//then if the member's set did not existed before we can assume that:
val (size1, size2, moved) = f2.runSyncUnsafe()
val (s1, s2, union, diff) = f2.runSyncUnsafe()
size1 shouldBe m1.size
s1.size shouldEqual (m1.size - 1)
size2 shouldBe m2.size
s2.size shouldEqual (m2.size + 1)
moved shouldBe true
s1 shouldNot contain theSameElementsAs m1
s2 shouldNot contain theSameElementsAs m2
union should contain theSameElementsAs m1 ++ m2
//although the list are not equal as at the beginning because of the move operation, its union still is the same
diff should contain theSameElementsAs List(m1.head)
//the difference between the k3 and k1 is equal to the element that was moved
```

### __SortedSets__

The following example uses the redis sorted sets api from `RedisSortedSet` to insert three scored elements into a redis sorted set, 
incrementing the middle one and then check that the scores are correctly reflected:

```scala
import monix.connect.redis.RedisSortedSet

//given
val k: String = "randomKey"
val v0: String = "v0"
val v1: String = "v1"
val v2: String = "v2"
val minScore: Double = 1
val middleScore: Double = 3 
val maxScore: Double = 4
val increment: Double = 2

//when
val t: Task[(ScoredValue[String], ScoredValue[String])] = for {
  _ <- RedisSortedSet.zadd(k, minScore, v0)
  _ <- RedisSortedSet.zadd(k, middleScore, v1)
  _ <- RedisSortedSet.zadd(k, maxScore, v2)
  _ <- RedisSortedSet.zincrby(k, increment, v1) //increments middle one by `increment` so it becomes the highest score of the set
  min <- RedisSortedSet.zpopmin(k)
  max <- RedisSortedSet.zpopmax(k) 
} yield (min, max)

//then we can confirm that:
val (min, max) = t.runSyncUnsafe()
min.getScore shouldBe minScore
min.getValue shouldBe v0
max.getScore shouldBe middleScore + increment
max.getValue shouldBe v1
```
### __Streams__

Coming soon.

### __Strings__

 The following example uses the redis keys api from `RedisString` to insert a string into the given key and get its size from redis
 
 ```scala
import monix.connect.redis.RedisString

 val ts: Task[Long] = for {
    _ <- RedisString.set(key, value).runSyncUnsafe()
    size <- RedisString.strlen(key)
   } yield size
ts.runToFuture() //eventually will return a failure if there was a redis server error, 0 if the key did not existed or the size of the string we put 
```

### __All in one__
 
 See below a complete demonstration on how to compose a different set of redis commands from different
 modules in the same for comprehension:

```scala
import monix.connect.redis.Redis
import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection

val redisClient: RedisClient = RedisClient.create("redis://host:port")
implicit val connection: StatefulRedisConnection[String, String] = redisClient.connect()
val k1: K
val value: V
val k2: K
val values: List[V] 
val k3: K

val t: Task[String, Long, List[V], List[K]] = {
  for {
    _ <- Redis.flushallAsync()            //removes all keys
    _ <- Redis.touch(k1)                  //creates the `k1`
    _ <- Redis.set(k1, value)             //insert the single `value` to `k2`
    _ <- Redis.rename(k1, k2)             //rename `k1` to `k2`
    _ <- Redis.lpush(k3, values: _*)      //push all the elements of the list to `k3`
    v <- Redis.get(k2)                    //get the element in `k2`
    _ <- Redis.lpushx(k3, v)              //pre-append v to the list in `k3`
    _ <- Redis.del(k2)                    //delete key `k2`
    len <- Redis.llen(k3)                 //lenght of the list
    l <- Redis.lrange(k3, 0, len).toListL //this is not safe unless you have a reasonable limit
    keys <- Redis.keys("*").toListL       //get all the keys
  } yield (v, len, l, keys)
}

//after this comprehnsion of redis operations it can be confirmed that:
val (v: String, len: Long, l: List[V], keys: List[K]) = t.runSyncUnsafe() // this is unsafe, and only used for testing purposes
v shouldBe value
len shouldBe values.size + 1
l should contain theSameElementsAs value :: values
keys.size shouldBe 1
keys.head shouldBe k3
```

## Local testing

The local tests will use the [redis docker image](https://hub.docker.com/_/redis/) from docker hub.

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

Finally, following code shows how you can create the redis connection to the local server, but
you would have to modify that to fit your use case - i.e it will be different to connect to a redis cluster or if authenticating to the server is needed using with key and secret, etc.)

```scala
val redisClient: RedisClient = RedisClient.create("redis://host:port")
implicit val connection: StatefulRedisConnection[String, String] = redisClient.connect()
``` 
And now you are ready to run your application! 

_Note that the above example defines the `connection` as `implicit`, since it is how the api will expect it._
