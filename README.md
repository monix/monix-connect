# Monix Connect        [![travis-badge][]][travis] 

 [travis]:                https://travis-ci.com/Cloriko/monix-connect
 [travis-badge]:          https://travis-ci.com/Cloriko/monix-connect.svg?branch=master
 
Monix Connect is an open source initiative to implement stream integrations for [Monix](https://monix.io/).
 A connector describes the connection between the application and a specific data point, which could be a file, a database or any system in which the appication 
 can interact by sending or receiving information. Therefore, the aim of this project is to catch the most common
  connections that users could need when developing reactive applications with Monix, these would basically reduce boilerplate code and furthermore, will let the users to greatly save time and complexity in their implementing projects.
  
See below the list of available [connectors](#Connectors).  

## Connectors
1. [Akka Streams](#Akka Streams)
2. [Common](#Akka)
3. [Parquet](#Parquet)
4. [DynamoDB](#DynamoDB)
5. [Redis](#Redis)
6. [S3](#S3)

### Akka Streams

### Common _(Not a connector)_

### Parquet

### DynamoDB
_Amazon DynamoDB_ is a key-value and document database that performs at any scale in a single-digit millisecond.
In which of the world's fastest growing enterprises depend on it to support their mission-critical workloads.

The DynamoDB operations availavle are: __create table__, __delete table__, __put item__, __get item__, __batch get__ and __batch write__, in which 
seen under the java api prespective, all of them inherit from `DynamoDbRequest` and `DynamoDbResponse` respectively for requests and responses.

Therefore, `monix-dynamodb` makes possible to use a generic implementation of `Observable` __transformer__ and __consumer__ that handles with any DynamoDB request available in the `software.amazon.awssdk`. 

See below an example of transforming and consuming DynamoDb operations with monix.

Required import: `scalona.monix.connect.dynamodb.DynamoDb`
 
Transformer:
```
Observable
.fromIterable(dynamoDbRequests) 
.transform(DynamoDb.transofrmer()) //for each element transforms the request operations into its respective response 
//the resulted observable would be of type Observable[Task[DynamoDbRequest]]
```

Consumer: 

```
Observable
.fromIterable(dynamoDbRequests)
.consumeWith(DynamoDb.consumer()) //a safe and syncronous consumer that executes each dynamodb request passed  
//the materialized value would be Task[DynamoDBResponse]
```

### Redis
_Redis_ is an open source, in-memory data structure store, used as a database, cache and message broker providing high availability, scalability and a outstanding performance. 
It supports data structures such as string, hashes, lists, sets, sorted sets with range queries, streams and more.
There are a set of [commands](https://redis.io/commands) already defined to inter-operate with Redis, in which most of them are also available from the java api.

The most common java library used for inter-operating with Redis from [lettuce](https://lettuce.io/), it defines fully non blocking Redis client built with netty that provides Reactive, Asyncronous and Syncronous Data Access.

So here is where `monix-redis` comes in, it is built on top of lettuce and allows the developer to avoid the boilerplate 
needed for operating with the lettuce _reactive_ and _async_ apis that returns the
 [Reactor](https://projectreactor.io/docs/core/release/reference/) reactive streams implementation on form of 
 (`Mono<T>` and `Flux<T>`) and `RedisFuture[T]` respectively. At the same time that the returning values
 are from scala lang and not form java, making it nicer to work with.
 
 The whole implementations can be found in `scalona.monix.connect.redis.Redis`, in which all af them takes type parameters that would represent the
 redis key `K` and value `V` type. It will also find an implicit value of type `StatefulRedisConnection[K, V]`, in which 
 the user can pass the implementation to use such as _Cluster_, _PubSub_, _MasterSlave_...
 
 Below you can find a table that shows the mapping between the java lettuce api to scala monix by using as an example
 some of the most common redis operations. 
 
  |  | Lettuce _Async_ | Lettuce _Reactive_ | _Monix_ |
  | :---: | :---: | :---: | :---: |
  | __del__ | _RedisFuture<java.lang.Long>_ | _Mono<java.lang.Long>_ | _Task[scala.Long]_  |
  | __hset__ | _RedisFuture<java.lang.Boolean>_ | _Mono<java.lang.Boolean>_ | _Task[scala.Boolean]_ |
  | __hgetall__ | _RedisFuture<java.utli.Map<K, V>>_ | _Mono<java.utli.Map<K, V>>_ |  _Task[collection.immutable.Map[K, V]_ |
  | __hkeys__ | _RedisFuture<java.utli.List<K>>_ | _Mono<java.utli.List<K, V>>_ | _Task[collection.immutable.List[K, V]_ |
  | __hmget__ | _RedisFuture<java.utli.List<KeyValue<K, V>>>_ | _Flux<KeyValue<K, V>>_ | _Observable[KeyValue[K, V]_ |
  | __...__ |  | |  |

### S3
Amazon Simple Storage Service (S3) is an object storage service that offers industry leading scalability, availability, security and performance.
It allows data storage of any amount of data, commonly used as a Data Lake for Big Data applications.


