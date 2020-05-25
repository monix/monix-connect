# Monix Connect  

[![release-badge][]][release] [![workflow-badge][]][workflow] 
[![Gitter](https://badges.gitter.im/monix/monix-connect.svg)](https://gitter.im/monix/monix-connect?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)




 [workflow]:                https://github.com/monix/monix-connect/actions?query=branch%3Amaster+workflow%3Abuild
 [workflow-badge]:          https://github.com/monix/monix-connect/workflows/build/badge.svg

   
 [release]:                 https://search.maven.org/search?q=a:monix-connect*
 [release-badge]:           https://img.shields.io/github/v/tag/monix/monix-connect.svg
 
 _Warning:_ Mind that the project is yet in early stages and its API is likely to be changed.
 
Monix Connect is an initiative to implement stream integrations for [Monix](https://monix.io/).
 A connector describes the connection between the application and a specific data point, which could be a file, a database or any system in which the appication 
 can interact by sending or receiving information. Therefore, the aim of this project is to catch the most common
 connections that users could need when developing reactive applications with Monix, these would basically reduce boilerplate code and furthermore, will let the users to greatly save time and complexity in their implementing projects.
 
 The latest stable version of `monix-connect` is compatible with Monix 3.x, Scala 2.12.x and 2.13.x, you can import 
 all of the connectors by adding the following dependency (find and fill your release [version](https://github.com/monix/monix-connect/releases)):
 
 ```scala   
 libraryDependencies += "io.monix" %% "monix-connect" % "VERSION"
```

But you can also only include to a specific connector to your library dependencies, see below how to do so and how to get started with each of the available [connectors](#Connectors).  

---

## Connectors
1. [Akka](#Akka)
2. [DynamoDB](#DynamoDB)
3. [Hdfs](#HDFS)
4. [Parquet](#Parquet)
5. [Redis](#Redis)
6. [S3](#S3)
7. [GCS](#GCS)
8. [Common](#Common)

---
## Akka

### Introduction

This module makes interoperability with akka streams easier by simply defining implicit extended classes for reactive stream conversions between akka and monix.

The three main core abstractions defined in akka streams are _Source_, _Flow_ and _Sink_, 
they were designed following the JVM [reactive streams](https://github.com/reactive-streams/reactive-streams-jvm/blob/v1.0.3/README.md) standards, 
meaning that under it's design they implement the `Publisher` and `Subscriber` contract. 
Which at the end it means that they can interoperate with other libraries that also implements the reactive stream specification, such as [Monix Reactive](https://monix.io/api/3.2/monix/reactive/index.html).
 So this module aims to provide a nice interoperability between these two reactive streams libraries.
 
 In order to achieve it, this module will provide an extended conversion method for each of the stream abstractions mentioned before. 
  These implicit extended methods can be imported from: `monix.connect.akka.stream.Converters._`.
Therefore, under the scope of the import, the signatures `.asObservable` and `.asConsumer` will be available for the `Source`, `Flow`, and `Sink`.

The below table shows in more detail the specs for the conversion from akka stremas to monix:  

  | _Akka_ | _Monix_ | _Akka -> Monix_ | _Monix -> Akka_ |
  | :---: | :---: | :---: | :---: | 
  | _Source[+In, +Mat]_ | _Observable[+In]_ | `source.asObservable[In]` | `observable.asSource[In]` |
  | _Flow[-In, +Out, +Mat]_ | _Consumer[-In, +Out]_ | `flow.asConsumer[Out]` | - |
  | _Sink[-In, +Out <: Future[Mat]]_ | _Consumer[-In, +Mat]_ | `sink.asConsumer[Mat]` | `consumer.asSink[In]` |

Note that two methods does not need to be typed as it has been done explicitly in the example table, the compiler will infer it for you.

Also, in order to perform these conversion it is required to have an implicit instance of `akka.stream.Materializer` and `monix.execution.Scheduler` in the scope.

### Set up

Add the following dependency to get started:
```scala 
libraryDependencies += "io.monix" %% "monix-akka" % "0.1.0"
```

### Getting started

The following code shows how these implicits can be initialized, but `Scheduler` and `ActorSystem` can be initialized differently and configured differently depending on the use case.
```scala
import monix.connect.akka.stream.Converters._
import monix.execution.Scheduler.Implicits.global

val actorSystem: ActorSystem = ActorSystem("Akka-Streams-InterOp") 
implicit val materializer = ActorMaterializer() 
```

#### Akka -> Monix

Let's see an example for converting an `Source[+In, +Mat]` to `Observable[+In]`:

```scala
//given
val elements = 1 until 50

//when
val ob: Observable[Int] = Source.from(elements).asObservable //`asObservable` converter as extended method of source.

//then 
ob.toListL.runSyncUnsafe() should contain theSameElementsAs elements
```

In this case we have not needed to consume the `Observable` since we directly used an operator that collects 
to a list `.toList`, but note that in case you need to use an specific consumer, you can also directly call `consumeWith`, as a shortcut for `source.asObservable.consumeWith(consumer)`, see an example below:

```scala
//given the same `elements` and `source` as above example`

//then
val t: Task[List[Int]] = source.consumeWith(Consumer.toList) //`consumeWith` as extended method of `Source`

//then
t.runSyncUnsafe() should contain theSameElementsAs elements
```

On the other hand, see how to convert an `Sink[-In, +Out <: Future[Mat]]` into a `Consumer[+In, +Mat]`.

```scala
//given
val foldSumSink: Sink[Int, Future[Int]] = Sink.fold[Int, Int](0)((acc, num) => acc + num)

//when
val (consumer: Consumer[Int, Int]) = foldSumSink.asConsumer[Int] //`asConsumer` as an extended method of `Sink`
val t: Task[Int] = Observable.fromIterable(Seq(1, 2, 3)).consumeWith(consumer)

//then the future value of `t` should be 6
```

Finally, you can also convert `Flow[-In, +Out, +Mat]` into `Consumer[+In, +Out]` in the same way you did with `Sink`
 in the previous example.

```scala
//given
val foldSumFlow: Flow[Int, Int, NotUsed] = Flow[Int].fold[Int](0)((acc, num) => acc + num)

//when
val (consumer: Consumer[Int, Int]) = foldSumFlow.asConsumer //`asConsumer` as an extended method of `Flow`
val t: Task[Int] = Observable.fromIterable(Seq(1, 2, 3)).consumeWith(consumer)

//then the future value of `t` should be 6
```

Notice that this interoperability would allow the Monix user to take advantage of the already pre built integrations 
from [Alpakka](https://doc.akka.io/docs/alpakka/current/index.html) or any other Akka Streams implementation.

#### Monix -> Akka

On the other hand, for converting from Monix `Observable[+In]` to Akka Streams `Source[+In, NotUsed]` we would use the conversion signature `asSource`.

```scala
import monix.connect.akka.stream.Converters._
val f: Future[Seq[Long]] = Observable.range(1, 100).asSource.runWith(Sink.seq) 
//eventualy will return a sequence from 1 to 100
```

Finally, for converting from `Sink[-In, +Out <: Future[Mat]]` to `Consumer[-In, +Mat]`. 

```scala
//given
import monix.connect.akka.stream.Converters._
val sink: Sink[Int, Future[String]] = Sink.fold[String, Int]("")((s, i) => s + i.toString)
val t: Task[String] = Observable.fromIterable(1 until 10).consumeWith(sink.asConsumer[String])
//eventually will return "123456789"
```

---
## DynamoDB

### Introduction

_Amazon DynamoDB_ is a key-value and document database that performs at any scale in a single-digit millisecond,
a key component for many platforms of the world's fastest growing enterprises that depend on it to support their mission-critical workloads.
   
The DynamoDB api provides a large list of operations (create, describe, delete, get, put, batch, scan, list and more...), all them are simply designed in request and response pattern, 
in which from the api prespective, they have to respectively implement [DynamoDbRequest](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/dynamodb/model/DynamoDbRequest.html) and [DynamoDbResponse](https://sdk.amazonaws.com/java/api/2.0.0/software/amazon/awssdk/services/dynamodb/model/DynamoDbResponse.html).  

The fact that all types implements either Request or Response type makes possible to create on top of that an abstraction layer that executes the received requests and retunrn a future value with the respective response.
 
From there one, this connector provides two pre built implementations of a monix __transformer__ and __consumer__ that implements the mentioned pattern and therefore allowing the user to use them for any 
 given dynamodb operation.  

### Set up

Add the following dependency to get started:
```scala 
libraryDependencies += "io.monix" %% "monix-dynamodb" % "0.1.0"
```

### Getting started

 This connector has been built on top of the [DynamoDbAsyncClient](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/dynamodb/DynamoDbAsyncClient.html) since it only exposes non blocking operations,
  that will permit the application to be authenticated and create an channel with AWS DynamoDB service.

The `DynamoDbAsyncClient` needs to be defined as `implicit` as it will be required by the _consumer_ and _tranformer_ implementations. 

It is also required and additional import for bringing the implicit conversions between DynamoDB requests and `DynamoDbOp`, the upper abstraction layer will allow to exacute them all (no need to worry about that):
 
  ```scala
import monix.connect.dynamodb.DynamoDbOp._
```
 
See below an example for transforming and consuming DynamoDb operations with monix.

_Transformer:_
```scala
//this is an example of a stream that transforms and executes DynamoDb `GetItemRequests`:
val dynamoDbRequests = List[GetItemRequest] = ???

val ob = Observable[Task[GetItemResponse]] = {
  Observable
    .fromIterable(dynamoDbRequests) 
    .transform(DynamoDb.transofrmer())
} //for each element transforms the get request operations into its respective get response 
//the resulted observable would be of type Observable[Task[GetItemResponse]]
```

_Consumer:_ 
```scala
//this is an example of a stream that consumes and executes DynamoDb `PutItemRequest`:
val putItemRequests = List[PutItemRequests] = ???

Observable
.fromIterable(dynamoDbRequests)
.consumeWith(DynamoDb.consumer()) //a safe and syncronous consumer that executes dynamodb requests  
//the materialized value would be of type Task[PutItemResponse]
```

Note that both transformers and consumer builder are generic implementations for any `DynamoDbRequest`, so you don't need
to explicitly specify its input and output types. 

### Local test environment - Set up

[Localstack](https://github.com/localstack/localstack) provides a fully functional local AWS cloud stack that in this case
the user can use to develop and test locally and offline the integration of the application with DynamoDB.

Add the following service description to your `docker-compose.yaml` file:

```yaml
dynamodb:
  image: localstack/localstack:latest
  ports:
    - '4569:4569'
  environment:
    - SERVICES=dynamodb
```

Run the following command to build, and start the dynamodb service:

```shell script
docker-compose -f docker-compose.yml up -d dynamodb
``` 

Check out that the service has started correctly.

Finally create the client to connect to the local dynamodb via `DynamoDbAsyncClient`:

```scala
val defaultAwsCredProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x"))
implicit val client: DynamoDbAsyncClient = {
  DynamoDbAsyncClient
    .builder()
    .credentialsProvider(defaultAwsCredProvider)
    .endpointOverride(new URI("http://localhost:4569"))
    .region(Region.AWS_GLOBAL)
    .build()
  }
``` 
You are now ready to run your application! 
_Note that the above example defines the client as `implicit`, since it is how the api will expect it._

---

### GCS - Google Cloud Storage

_Cloud Storage_ provides worldwide, highly durable object storage that scales to exabytes of data.
You can access data instantly from any storage class
```scala
import java.io.File

import io.monix.connect.gcs._
import io.monix.connect.gcs.configuration._

// Create a new Storage Bucket
val config: BucketConfig = BucketConfig("mybucket")
val bucket: Task[Bucket] = Bucket(config)

// Upload a File
val file0 = new File("/tmp/myfile0.txt")
val metadata = BlobInfo(
  contentType = Some("text/plain")
)

val blob0: Task[Blob] = {
  for {
    bucket <- bucket
    blob   <- b.upload(file0, metadata)
  } yield blob
}

// Download a Blobs content
val file1 = new File("/tmp/myfile1.txt")
val blob1 = {
  for {
    blob <- blob0
    _    <- blob.downloadTo(file1.getPath)
  } yield ()
}

```

---

## HDFS

### Introduction

The _Hadoop Distributed File System_ ([HDFS](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html)) is a distributed file system designed to run on commodity hardware, 
it is is highly fault-tolerant and it provides high throughput access which makes it suitable for applications that have to handle with large data sets.

This connector then allows to read and write hdfs files of any size in a streaming fashion.

The methods to perform these operations are exposed under the object ```monix.connect.hdfs.Hdfs```, in which
it has been built on top of the the official _apache hadoop_ api.  


### Set up

Add the following dependency to get started:
```scala 
libraryDependencies += "io.monix" %% "monix-hdfs" % "0.1.0"
```

By default the connector uses Hadoop version 3.1.1. In case you need a different one you can replace it by excluding `org.apache.hadoop` from `monix-hdfs` and add the new one to your library dependencies.


### Getting started

The following import is a common requirement for all those methods defined in the `Hdfs` object:
```scala
import org.apache.hadoop.fs.FileSystem
//The abstract representation of a file system which could be a distributed or a local one.
import org.apache.hadoop.fs.Path
//Represents a file or directory in a FileSystem
```

Each use case would need different settings to create the hadoop configurations, but 
 for testing purposes we would just need a plain one: 
 
```scala
val conf = new Configuration() //Provides access to the hadoop configurable parameters
conf.set("fs.default.name", s"hdfs://localhost:$port") //especifies the local endpoint of the test hadoop minicluster
val fs: FileSystem = FileSystem.get(conf)
```
 
Then we can start interacting with hdfs, the following example shows how to construct a pipeline that reads from the specified hdfs file.

```scala

val sourcePath: Path = new Path("/source/hdfs/file_source.txt")
val chunkSize: Int = 8192 //size of the chunks to be pulled

//Once we have the hadoop classes we can create the hdfs monix reader
val ob: Observable[Array[Byte]] = Hdfs.read(fs, path, chunkSize)
```

Since using hdfs means we are dealing with big data, it makes it difficult or very expensive to read the whole file at once,
 therefore with the above example we will read the file in small parts configured by `chunkSize` that eventually will end with the whole file being processed. 

Using the stream generated when reading form hdfs in the previous we could write them back into a path like:
 ```scala
val destinationPath: Path = new Path("/destination/hdfs/file_dest.txt")
val hdfsWriter: Consumer[Array[Byte], Task[Long]] = Hdfs.write(fs, destinationPath) 

// eventually it will return the size of the written file
val t: Task[Long] = ob.consumeWith(hdfsWriter) 
 ```
The returned type would represent the total size in bytes of the written data.

Note that the write hdfs consumer implementation provides different configurations to be passed as parameters such as 
enable overwrite (true by default), replication factor (3), the bufferSize (4096 bytes), blockSize (134217728 bytes =~ 128 MB) 
and finally a line separator which is not used by default (None).

Below example shows an example on how can them be tweaked:

```scala
val hdfsWriter: Consumer[Array[Byte], Long] = 
   Hdfs.write(fs,
      path = path, 
      overwrite = false, //will fail if the path already exists
      replication = 4, 
      bufferSize = 4096,
      blockSize =  134217728, 
      lineSeparator = "\n") //each written element would include the specified line separator 
```        

Finally, the hdfs connector also exposes an append operation, in which in this case it materializes to a `Long`
that would represent the only the size of the appended data, but not of the whole file. 

Note also that this method does not allow to configure neither the replication factor nor block size and so on, this is because
these configurations are only set whenever a file is created, but an append operation would reuse them from the existing file.

See below an example:

```scala
// you would probably need to tweak the hadoop configuration to allow the append operation
conf.setBoolean("dfs.support.append", true)
conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER") 

// note that we are re-using the `destinationPath` of the last example since should already exist
val hdfsAppender: Consumer[Array[Byte], Task[Long]] = Hdfs.append(fs, destinationPath) 
val ob: Observer[Array[Byte]] = ???
val t: Task[Long] = ob.consumeWith(hdfsAppender) 
```
 
### Local environment - Set up
 
 Apache Hadoop has a sub project called [Mini Cluster](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-minicluster) 
 that allows to locally spin up a single-node Hadoop cluster without the need to set any environment variables or manage configuration files.
  
Add to your library dependencies with the desired version:
 
```scala
"org.apache.hadoop" % "hadoop-minicluster" % "VERSION" % Test
```

From there on, since in this case the tests won't depend on a docker container but as per using a library dependency they will run against the JVM, so you will have to specify where to start and stop the 
hadoop mini cluster on the same test, it is a good practice do that on `BeforeAndAfterAll`:

```scala
import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}

private var miniHdfs: MiniDFSCluster = _
private val dir = "./temp/hadoop" //sample base dir where test data will be stored
private val port: Int = 54310 
private val conf = new Configuration()
conf.set("fs.default.name", s"hdfs://localhost:$port")
conf.setBoolean("dfs.support.append", true)

override protected def beforeAll(): Unit = {
  val baseDir: File = new File(dir, "test")
  val miniDfsConf: HdfsConfiguration = new HdfsConfiguration
  miniDfsConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
  miniHdfs = new MiniDFSCluster.Builder(miniDfsConf)
    .nameNodePort(port)
    .format(true)
    .build()
  miniHdfs.waitClusterUp()
}

override protected def afterAll(): Unit = {
  fs.close()
  miniHdfs.shutdown()
}
```

---
## Parquet

### Introduction
[Apache Parquet](http://parquet.apache.org/) is a columnar storage format that provides the advantages of compressed, efficient data representation available to any project in the Hadoop ecosystem.

 It has already been proved by multiple projects that have demonstrated the performance impact of applying the right compression and encoding scheme to the data.
  
Therefore, the parquet connector basically exposes stream integrations for reading and writing into and from parquet files either in the _local system_, _hdfs_ or _S3_.
 
### Set up

Add the following dependency:
 
 ```scala
 libraryDependencies += "io.monix" %% "monix-parquet" % "0.1.0"
 ```

### Getting started

These two signatures `Parquet.write` and `Parquet.read` are built on top of the _apache parquet_  `ParquetWriter[T]` and `ParquetReader[T]`, therefore they need an instance of these types to be passed.

The below example shows how to construct a parquet consumer that expects _Protobuf_ messages and pushes 
them into the same parquet file of the specified location.

```scala
import monix.connect.parquet.Parquet
import org.apache.hadoop.fs.Path
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
import monix.connect.parquet.Parquet
import org.apache.hadoop.fs.Path
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

Warning: This connector provides with the logic of building a publisher and subscriber from a given apache hadoop `ParquetReader` and `ParquetWriter` respectively,
but it does not cover any existing issue within the support interoperability of the apache parquet library with external ones.
Notice that p.e we have found an issue when reading parquet as protobuf messages with `org.apache.parquet.hadoop.ParquetReader` but not when writing.
Follow the state of this [issue](https://github.com/monix/monix-connect/issues/34).
On the other hand, it was all fine the integration between `Avro` and `Parquet`. 

### Local test environment - Set up

It will depend on the specific use case, as we mentioned earlier in the introductory section it can operate on the local filesystem on hdfs or even in S3.

Therefore, depending on the application requirements, the hadoop `Configuration` class will need to be configured accordingly.
 
__Local:__ So far in the examples has been shown how to use it locally, in which in that case it would just be needed to create a plain instance like: ```new org.apache.hadoop.conf.Configuration()``` and the local path will be 
specified like: `new org.apache.hadoop.fs.Path("/this/represents/a/local/path")`.

__Hdfs:__ On the other hand, the most common case is to work with parquet files in hdfs, in that case my recommendation is to find specific posts and examples on how to set up your configuration for that.
But on some extend, for setting up the local test environment you would need to use the hadoop minicluster and set the configuration accordingly. 
You can check the how to do so in the `monix-hdfs` documentation. 

__S3:__ Finally, integrating the parequet connector with AWS S3 requires [specific configuration values](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html) to be set. On behalf of configuring it 
to run local tests ... 
Note that you will also require to spin up a docker container for emulating the AWS S3 service, check how to do so in the `monix-s3` documentation. 

---
## Redis
### Introduction
_Redis_ is an open source, in-memory data structure store, used as a database, cache and message broker providing high availability, scalability and a outstanding performance. 
It supports data structures such as string, hashes, lists, sets, sorted sets with range queries, streams and more.
It has a defined a set of [commands](https://redis.io/commands) to inter-operate with, and most of them are also available from the java api.

This connector has been built on top of [lettuce](https://lettuce.io/), the most popular java library for operating with a non blocking Redis client.


Then `monix-redis` creates the interoperability between the reactive types returned by the lettuce api like (`Mono<T>` and `Flux<T>`) from  [Reactor](https://projectreactor.io/docs/core/release/reference/) or `RedisFuture[T]`
At the same time that it returns the right values from scala lang and not form java, resulting in a greatly reduction of boilerplate code that makes the user to have a nice experience while integrating 
 redis operations using monix.
 
 See an example in below table: 
 
  | Signature | Lettuce _Async_ | Lettuce _Reactive_ | _Monix_ |
  | :---: | :---: | :---: | :---: |
  | __del__ | _RedisFuture<java.lang.Long>_ | _Mono<java.lang.Long>_ | _Task[Long]_  |
  | __hset__ | _RedisFuture<java.lang.Boolean>_ | _Mono<java.lang.Boolean>_ | _Task[Boolean]_ |
  | __hvals__ | _RedisFuture<java.utli.List<V>>_ | _Flux<V<V>>_ | _Observable[V]_ |
  | __...__ | ... | ... | ... |
  
  
#### Set up

Add the following dependency:

```scala
libraryDependencies += "io.monix" %% "monix-redis" % "0.1.0"
```

### Getting started

Redis provides a wide range of commands to perform a different range of operations, in which it has been splitted between 15 different groups. 
Monix Redis connector only currently provides support for the most common used ones:  ([Keys](https://redis.io/commands#generic), [Hashes](https://redis.io/commands#hash), [List](https://redis.io/commands#list), [Pub/Sub](https://redis.io/commands#pubsub), [Server](https://redis.io/commands#server), [Sets](https://redis.io/commands#set), [SortedSets](https://redis.io/commands#sorted_set), [Streams](https://redis.io/commands#stream) and [Strings](https://redis.io/commands#string)).
Each of these modules has its own object located under `monix.connect.redis`, being `Redis` the one that aggregates them all. But they can be individually used too.

The only extra thing you need to do for start using it is to have an implicit `StatefulRedisConnection[K, V]` in the scope. 
 
On continuation let's show an example for each of the redis data group:
 
__Keys__
 
The following example uses the redis keys api `monix.connect.redis.RedisKey` to show a little example on working with some basic key operations.

```scala
//given two keys and a value
val key1: K //assuming that k1 initially exists
val key2: K //k2 does not exists
val value: String

//when
val f: Task[Long, Boolean, Long, Long, Long, Long] = {
  for {
    initialTtl <- RedisKey.ttl(key1) //checks the ttl when it hasn't been set yet
    expire <-  RedisKey.expire(key1, 5) //sets the ttl to 5 seconds
    finalTtl <- RedisKey.ttl(key1) //checks the ttl again
    existsWithinTtl <- RedisKey.exists(key1) //checks whether k1 exists or not
    _ <- RedisKey.rename(key1, key2) //renames k1 to k2
    existsRenamed <- RedisKey.exists(key2) //checks that it exists after being renamed
    _ <- Task.sleep(6.seconds)
    existsAfterFiveSeconds <- RedisKey.exists(key2) //after 6 seconds checks ttl again
  } yield (initialTtl, expire, finalTtl, existsWithinTtl, existsRenamed, existsAfterFiveSeconds)
}

//then
val (initialTtl, expire, finalTtl, existsWithinTtl, existsRenamed, existsAfterFiveSeconds).runSyncUnsafe()
initialTtl should be < 0L
finalTtl should be > 0L
expire shouldBe true
existsWithinTtl shouldBe 1L
existsRenamed shouldBe 1L
existsAfterFiveSeconds shouldBe 0L
```

__Hashes__

The following example uses the redis hash api `monix.connect.redis.RedisHash` to insert a single element into a hash and read it back from the hash.

```scala
val key: String = ???
val field: String = ???
val value: String = ???
 
val t: Task[String] = for {
    _ <- RedisHash.hset(key, field, value).runSyncUnsafe() 
    v <- RedisHash.hget(key, field)
} yield v
   
val fv: Future[String] = t.runToFuture()
```

__Lists__

The following example uses the redis list api `monix.connect.redis.RedisList` to insert elements into a redis list and reading them back with limited size.

```scala
val key: String = String
val values: List[String]
  
val tl: Task[List[String]] = for {
  _ <- RedisList.lpush(key, values: _*)
  l <- RedisList.lrange(key, 0, values.size).toListL
} yield l
  
//a different alternative to use whether there is a risk of fetching a big list of elements
//or that you want to keep working with Observable[v] type rather than Task[List[V]]
val ob: Observable[String] = for {
  _ <- Observable.fromTask(RedisList.lpush(key, values: _*))
  ob <- RedisList.lrange(key, 0, values.size)
} yield ob
```

__Pub/Sub__

Example coming soon.

__Server__

The following code shows how to remove all keys from all dbs in redis using the server api `monix.connect.redis.RedisServer` a very basic but also common use case: 

```scala
val t: Task[String] = RedisServer.flushall() //returns a simple string reply
```

__Sets__
 
The following code uses the redis sets api from `monix.connect.redis.RedisSet`, this one is a bit longer than the others but not more complex. 

```scala
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

__SortedSets__

The following example uses the redis sorted sets api `monix.connect.redis.RedisSortedSet` to insert three scored elements into a redis sorted set, 
incrementing the middle one and then check that the scores are correctly reflected:

```scala
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
RedisSortedSet.zadd(k, minScore, v0)
val t: Task[(ScoredValue[String], ScoredValue[String])] = for {
  _ <- RedisSortedSet.zadd(k, minScore, v0)
  _ <- RedisSortedSet.zadd(k, middleScore, v1)
  _ <- RedisSortedSet.zadd(k, maxScore, v2)
  _ <- RedisSortedSet.zincrby(k, increment, v1) //increments middle one by `increment` so it becomes the highest score of the set
  min <- RedisSortedSet.zpopmin(k)
  max <- RedisSortedSet.zpopmax(k) 
} yield (min, max)

//then we can assume that:
val (min, max) = t.runSyncUnsafe()
min.getScore shouldBe minScore
min.getValue shouldBe v0
max.getScore shouldBe middleScore + increment
max.getValue shouldBe v1
```
__Streams__

Example coming soon.

__Strings__

 The following example uses the redis keys api `monix.connect.redis.RedisString` to insert a string into the given key and get its size from redis
 
 ```scala
 val ts: Task[Long] = for {
    _ <- RedisString.set(key, value).runSyncUnsafe()
    size <- RedisString.strlen(key)
   } yield size
ts.runToFuture() //eventually will return a failure if there was a redis server error, 0 if the key did not existed or the size of the string we put 
```

 __All in one__
 
 See below a complete demonstration on how to compose a different set of Redis commands from different 
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

val (v: String, len: Long, l: List[V], keys: List[K]) = {
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
}.runSyncUnsafe() // this is unsafe, and only used for testing purposes

//after this comprehnsion of redis operations it can be confirmed that:
v shouldBe value
len shouldBe values.size + 1
l should contain theSameElementsAs value :: values
keys.size shouldBe 1
keys.head shouldBe k3
```

### Local test environment - Set up

The local tests will be use the [redis docker image](https://hub.docker.com/_/redis/).

Add the following service description to your `docker-compose.yaml` file:

```yaml
 redis:
    image: redis
    ports:
      - 6379:6379
```

Run the following command to build, and start the redis server:

```shell script
docker-compose -f docker-compose.yml up -d redis
``` 

Check out that the service has started correctly.

Finally, as you might have seen in some of the examples, you can create the redis connection just by:

```scala
val redisClient: RedisClient = RedisClient.create("redis://host:port")
implicit val connection: StatefulRedisConnection[String, String] = redisClient.connect()
``` 
And now you are ready to run your application! 

_Note that the above example defines the client as `implicit`, since it is how the api will expect it._

---
## S3

### Introduction

The object storage service that offers industry leading scalability, availability, security and performance.
It allows data storage of any amount of data, commonly used as a data lake for big data applications which can now be easily integrated with monix.
 
 The module has been implemented using the `S3AsyncClient` since it only exposes non blocking methods. 
 Therefore, all of the monix s3 methods defined in the `S3` object would expect an implicit instance of 
 this class to be in the scope of the call.
   
### Set up
 
 Add the following dependency:
 
 ```scala
 libraryDependencies += "io.monix" %% "monix-s3" % "0.1.0"
 ```

### Getting started 

 First thing is to create the s3 client that will allow us to authenticate and create an channel between our 
 application and the AWS S3 service. 
 
 So the below code shows an example on how to set up this connection. Note that in this case 
  the authentication is done thorugh AWS S3 using access and secret keys, 
  but you might use another method such as IAM roles.
 
 ```scala
import java.net.URI
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.regions.Region.AWS_GLOBAL

val basicAWSCredentials: AwsBasicCredentials = AwsBasicCredentials.create(s3AccessKey, s3SecretKey)
val credentialsProvider: StaticCredentialsProvider = StaticCredentialsProvider.create(basicAWSCredentials)

// Note that the client is defined as implicit, this is on purpose since each of the methods defined in
// the monix s3 connector will expect that.
 implicit val s3Client: S3AsyncClient = S3AsyncClient
    .builder()
    .credentialsProvider(credentialsProvider)
    .region(AWS_GLOBAL)
    .endpointOverride(URI.create(endPoint))//this one is used to point to the localhost s3 service, not used in prod 
    .build
```
 
Once we have configured the s3 client, let's start with the basic operations to _create_ and _delete_ buckets:
 ```scala
import software.amazon.awssdk.services.s3.model.{CreateBucketResponse, DeleteBucketResponse}

val bucketName: String = "myBucket" 
val _: Task[CreateBucketResponse] = S3.createBucket(bucketName)
val _: Task[DeleteBucketResponse] = S3.deleteBucket(bucketName)
```

You can also operate at object level within a bucket with:
 ```scala
import software.amazon.awssdk.services.s3.model.{DeleteObjectResponse, ListObjectsResponse}

val bucketName: String = "myBucket" 
val _: Task[DeleteObjectResponse] = S3.deleteObject(bucketName)
val _: Task[ListObjectsResponse] = S3.listObjects(bucketName)
```

On the other hand, to get and put objects:
 ```scala
import software.amazon.awssdk.services.s3.model.PutObjectResponse

val bucketName: String = "myBucket" 

//get example
val objectKey: String = "/object/file.txt"
val _: Task[Array[Byte]] = S3.getObject(bucketName, objectKey)

//put object example
val content: Array[Byte] = "file content".getBytes()
val _: Task[PutObjectResponse] = S3.putObject(bucketName, objectKey, content)
}
```

Finally, for dealing with large files of data you might want to use the `multipartUpload` consumer.
This one consumes an observable and synchronously makes partial uploads of the incoming chunks. 

Thus, it reduces substantially the risk on having jvm overhead errors or getting http requests failures, 
since the whole file does not need to be allocated in the memory and the http request body won't be that big. 

The partial uploads can be fine tuned by the minimum chunksize that will be sent, being 5MB the default minimum size (equally as an integer value of 5242880).

```scala
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse

// given an strem of chunks (Array[Byte]) 
val ob: Observable[Array[Byte]] = Observable.fromIterable(chunks)

// and a multipart upload consumer
val multipartUploadConsumer: Consumer[Array[Byte], Task[CompleteMultipartUploadResponse]] =
  S3.multipartUpload(bucketName, objectKey)

// then
ob.fromIterable(chunks).consumeWith(multipartUploadConsumer)
```

### Local test environment - Set up

For AWS S3 local testing we went with [minio](https://github.com/minio/minio) instead of localstack, since we found an [issue](https://github.com/localstack/localstack/issues/538) that can block you on writing your functional tests.

Add the following service description to your `docker-compose.yaml` file:

```yaml
minio:
  image: minio/minio
  ports:
    - "9000:9000"
  volumes:
    - ./minio/data:/data
  environment:
    - MINIO_ACCESS_KEY=TESTKEY
    - MINIO_SECRET_KEY=TESTSECRET
  healthcheck:
    test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
    interval: 35s
    timeout: 20s
    retries: 3
  command: server --compat /data
```

Run the following command to build, and start the redis server:

```shell script
docker-compose -f docker-compose.yml up -d minio
``` 
Check out that the service has started correctly, notice that a healthcheck has been defined on the description of the minio service, 
that's because minio s3 is a very heavy image and sometimes it takes too long to be set up or sometime it even fails, so that would prevent those cases.

Finally, create the connection with AWS S3, note that minio does not has support for `AnonymousCredentialsProvider`, 
therefore you'll have to use `AwsBasicCredentials`, in which the _key_ and _secret_ will correspond respectively to the
 defined environment variables from docker compose definition `MINIO_ACCESS_KEY` and `MINIO_SECRET_KEY`. 

```scala
import software.amazon.awssdk.regions.Region.AWS_GLOBAL
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}

val minioEndPoint: String = "http://localhost:9000"

val s3AccessKey: String = "TESTKEY" //see docker minio env var `MINIO_ACCESS_KEY`
val s3SecretKey: String = "TESTSECRET" //see docker minio env var `MINIO_SECRET_KEY`

val basicAWSCredentials = AwsBasicCredentials.create(s3AccessKey, s3SecretKey)
implicit val s3AsyncClient: S3AsyncClient = S3AsyncClient
  .builder()
  .credentialsProvider(StaticCredentialsProvider.create(basicAWSCredentials))
  .region(AWS_GLOBAL)
  .endpointOverride(URI.create(minioEndPoint))
  .build
``` 

Now you are ready to run your application! 

_Note that the above example defines the client as `implicit`, since it is how the api will expect this one._

---

### Contributing


The Monix Connect project welcomes contributions from anybody wishing to
participate.  All code or documentation that is provided must be
licensed with the same license that Monix Connect is licensed with (Apache
2.0, see LICENSE.txt).

People are expected to follow the
[Scala Code of Conduct](./CODE_OF_CONDUCT.md) when
discussing Monix on GitHub, Gitter channel, or other venues.

Feel free to open an issue if you notice a bug, you have a question about the code,
 an idea for an existing connector or even for adding a new one. Pull requests are also
gladly accepted. For more information, check out the
[contributor guide](CONTRIBUTING.md).

## License

All code in this repository is licensed under the Apache License,
Version 2.0. See [LICENCE.txt](./LICENSE.txt).

