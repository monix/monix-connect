# - Monix Connect - [![travis-badge][]][travis] 

 [travis]:                https://travis-ci.com/github/Cloriko/monix-connect
 [travis-badge]:          https://travis-ci.com/Cloriko/monix-connect.svg?branch=master
 
Monix Connect is an open source initiative to implement stream integrations for [Monix](https://monix.io/).
 A connector describes the connection between the application and a specific data point, which could be a file, a database or any system in which the appication 
 can interact by sending or receiving information. Therefore, the aim of this project is to catch the most common
  connections that users could need when developing reactive applications with Monix, these would basically reduce boilerplate code and furthermore, will let the users to greatly save time and complexity in their implementing projects.
  
See below the list of available [connectors](#Connectors).  

---

## Connectors
1. [Akka](#Akka)
2. [Parquet](#Parquet)
3. [Hdfs](#Hdfs)
4. [DynamoDB](#DynamoDB)
5. [Redis](#Redis)
6. [S3](#S3)
2. [Common](#Common)

---
### Akka
This module makes interoperability with akka streams easier by simply defining implicit extended classes for reactive stream conversions between akka and monix.

These implicit extended classes needs to be imported from: `cloriko.monix.connect.akka.Implicits._`.
Therefore, under the scope of the import the signatures `.asObservable` and `.asConsumer` would be available from the `Source`, `Flow`, and `Sink`.
The two methods does not need to be typed as it has been done explicitly in the example table, the compiler will infer it for you.

The below table shows these conversions in more detail:  

  | _Akka_ | _Monix_ | _Using_ |
  | :---: | :---: | :---: | 
  | _Source[+In, +Mat]_ | _Observable[+In]_ | `source.asObservable[In]` |
  | _Flow[+In, -Out,+Mat]_ | _Consumer[+In, Task[-Out]]_ | `flow.asConsumer[Out]` |
  | _Sink[-In, +Out <: Future[Mat]]_ | _Consumer[In, Task[+Mat]_ | `sink.asConsumer[Mat]` |

Notice that this interoperability would allow the Monix user to take advantage of the already pre built integrations 
from [Alpakka](https://doc.akka.io/docs/alpakka/current/index.html) or any other Akka Streams implementation.

---
### Parquet

The is connector provides with stream integrations for reading and writing into and from parquet files in the _local system_, _hdfs_ or _s3_.
 
 These two signatures depends on a implementation of the _apache parquet_  `ParquetWriter[T]` and `ParquetReader[T]` to be passed.

The below example shows how to construct a parquet consumer that expects _Protobuf_ messages and pushes 
them into the same parquet file of the specified location.
```scala
import cloriko.monix.connect.parquet.Parquet
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.hadoop.conf.Configuration

val file: String = "/invented/file/path"
val conf = new Configuration()
val messages: List[ProtoMessage] 
val writeSupport = new ProtoWriteSupport[ProtoMessage](classOf[ProtoMessage])
val w = new ParquetWriter[ProtoMessage](new Path(file), writeSupport)
Observable
 .fromIterable(messages)
 .consumeWith(Parquet.writer(w))
//ProtoMessage implements [[com.google.protobuf.Message]]
```

On the other hand, the following code shows how to pull _Avro_ records from a parquet file:

```scala
import cloriko.monix.connect.parquet.Parquet
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.util.HadoopInputFile

val r: ParquetReader[AvroRecord] = {
 AvroParquetReader
  .builder[AvroRecord](HadoopInputFile.fromPath(new Path(file), conf))
  .withConf(conf)
  .build()
}

val ob: Observable[AvroRecord] = Parquet.reader(r)
//AvroRecord implements [[org.apache.avro.generic.GenericRecord]]
```
---
### DynamoDB
_Amazon DynamoDB_ is a key-value and document database that performs at any scale in a single-digit millisecond.
In which of the world's fastest growing enterprises depend on it to support their mission-critical workloads.

The DynamoDB operations availavle are: __create table__, __delete table__, __put item__, __get item__, __batch get__ and __batch write__, in which 
seen under the java api prespective, all of them inherit from `DynamoDbRequest` and `DynamoDbResponse` respectively for requests and responses.

Therefore, `monix-dynamodb` makes possible to use a generic implementation of `Observable` __transformer__ and __consumer__ that handles with any DynamoDB request available in the `software.amazon.awssdk`. 

See below an example of transforming and consuming DynamoDb operations with monix.

Required import: `scalona.monix.connect.dynamodb.DynamoDb`
 
Transformer:
```scala
Observable
.fromIterable(dynamoDbRequests) 
.transform(DynamoDb.transofrmer()) //for each element transforms the request operations into its respective response 
//the resulted observable would be of type Observable[Task[DynamoDbRequest]]
```

Consumer: 

```scala
Observable
.fromIterable(dynamoDbRequests)
.consumeWith(DynamoDb.consumer()) //a safe and syncronous consumer that executes dynamodb requests  
//the materialized value would be of type Task[DynamoDBResponse]
```
---
### Hdfs
A connector that allows to write and read from files of any size stored in [HDFS](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html).

The methods to perform these operations are exposed under the scala object ```cloriko.monix.connect.hdfs.Hdfs```, in which
it has been constructed on top of the _apache hadoop_ api.  

---
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


---
### S3
Amazon Simple Storage Service (S3) is an object storage service that offers industry leading scalability, availability, security and performance.
It allows data storage of any amount of data, commonly used as a Data Lake for Big Data applications.

---
