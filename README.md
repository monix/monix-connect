# Monix Connect  

[![travis-badge][]][travis] 
[![Gitter](https://badges.gitter.im/monix/monix-connect.svg)](https://gitter.im/monix/monix-connect?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

 [travis]:                https://travis-ci.com/github/monix/monix-connect
 [travis-badge]:          https://travis-ci.com/monix/monix-connect.svg?branch=master
 
 _Warning:_ This project is in early stages and the API might be likely to be changed in future next releases.
 
Monix Connect is an initiative to implement stream integrations for [Monix](https://monix.io/).
 A connector describes the connection between the application and a specific data point, which could be a file, a database or any system in which the appication 
 can interact by sending or receiving information. Therefore, the aim of this project is to catch the most common
 connections that users could need when developing reactive applications with Monix, these would basically reduce boilerplate code and furthermore, will let the users to greatly save time and complexity in their implementing projects.
 
See below the list of available [connectors](#Connectors).  

---

## Connectors
1. [Akka](#Akka)
2. [DynamoDB](#DynamoDB)
3. [Hdfs](#HDFS)
4. [Parquet](#Parquet)
5. [Redis](#Redis)
6. [S3](#S3)

---
### Akka
This module makes interoperability with akka streams easier by simply defining implicit extended classes for reactive stream conversions between akka and monix.

These implicit extended classes needs to be imported from: `monix.connect.akka.Implicits._`.
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
### DynamoDB
_Amazon DynamoDB_ is a key-value and document database that performs at any scale in a single-digit millisecond.
In which of the world's fastest growing enterprises depend on it to support their mission-critical workloads.

The __main__ DynamoDB operations availavle are: __create table__, __delete table__, __put item__, __get item__, __batch get__ and __batch write__, but all the defined under 
`software.amazon.awssdk.services.dynamodb.model` are available as well, more precisely all whose inherit from `DynamoDbRequest` and `DynamoDbResponse` respectively for requests and responses.

Therefore, `monix-dynamodb` makes possible to use a generic implementation of `Observable` __transformer__ and __consumer__ that handles with any DynamoDB request available in the `software.amazon.awssdk`. 

See below an example of transforming and consuming DynamoDb operations with monix.

Required import: `monix.connect.dynamodb.DynamoDbOp._`
 
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

See an example of a stream that consumes and executes DynamoDb `GetItemRequests`:

```scala
val ob: Observable[GetItemRequest] = ???
ob.consumeWith(DynamoDb.consumer())
```

---
### HDFS

A connector that allows to progresively write and read from files of any size stored in [HDFS](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html).

The methods to perform these operations are exposed under the scala object ```monix.connect.hdfs.Hdfs```, in which
it has been constructed on top of the the official _apache hadoop_ api.  

The following import is a common requirement for all those methods defined in the `Hdfs` object:
```scala
import org.apache.hadoop.fs.FileSystem
//The abstract representation of a file system which could be a distributed or a local one.
import org.apache.hadoop.fs.Path
//Represents a file or directory in a FileSystem
```

Each use case would need different settings to create the hadoop configurations, but 
 for testing purposes we would just need to set a couple of them. 
 
```scala
val conf = new Configuration() //Provides access to the hadoop configurable parameters
conf.set("fs.default.name", s"hdfs://localhost:$port") //especifies the local endpoint of the test hadoop minicluster
val fs: FileSystem = FileSystem.get(conf)
```
On continuation, an example on how to construct a pipeline that reads from the specified hdfs file.

```scala

val sourcePath: Path = new Path("/source/hdfs/file_source.txt")
val chunkSize: Int = 8192 //size of the chunks to be pulled

//Once we have the hadoop classes we can create the hdfs monix reader
val ob: Observable[Array[Byte]] = Hdfs.read(fs, path, chunkSize)
```
Now that we have a stream of bytes coming in, so it can be transformed as we want to.
And later it can be written back to a different hdfs path like:
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
Below example shows how to:
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
Note also that this method does not allow to configure the replication factor, block size and so on, this is because
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
 
---
### Parquet

The is connector provides with stream integrations for reading and writing into and from parquet files in the _local system_, _hdfs_ or _s3_.
 
 These two signatures depends on a implementation of the _apache parquet_  `ParquetWriter[T]` and `ParquetReader[T]` to be passed.

The below example shows how to construct a parquet consumer that expects _Protobuf_ messages and pushes 
them into the same parquet file of the specified location.
```scala
import monix.connect.parquet.Parquet
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
## Redis
#### Introduction
_Redis_ is an open source, in-memory data structure store, used as a database, cache and message broker providing high availability, scalability and a outstanding performance. 
It supports data structures such as string, hashes, lists, sets, sorted sets with range queries, streams and more.
There are a set of [commands](https://redis.io/commands) already defined to inter-operate with Redis, in which most of them are also available from the java api.

This connector has been built on top of [lettuce](https://lettuce.io/), the most popular java library for operating with a non blocking Redis client.
llows the developer to avoid the boilerplate 
needed for operating with the lettuce _reactive_ and _async_ apis that returns the

Them `monix-redis` creates the inter-operability between the reactive types returned by the lettuce api like (`Mono<T>` and `Flux<T>`) from  [Reactor](https://projectreactor.io/docs/core/release/reference/) or `RedisFuture[T]`
At the same time that it returns the right values from scala lang and not form java, resulting in a greatly reduction of boilerplate code that makes the user to have a nice experience while integrating 
 redis operations using monix.
 See an example in below table: 
 
  | Signature | Lettuce _Async_ | Lettuce _Reactive_ | _Monix_ |
  | :---: | :---: | :---: | :---: |
  | __del__ | _RedisFuture<java.lang.Long>_ | _Mono<java.lang.Long>_ | _Task[Long]_  |
  | __hset__ | _RedisFuture<java.lang.Boolean>_ | _Mono<java.lang.Boolean>_ | _Task[Boolean]_ |
  | __hvals__ | _RedisFuture<java.utli.List<V>>_ | _Flux<V<V>>_ | _Observable[V]_ |
  | __...__ | ... | ... | ... |
  
#### Getting started

The Redis provides a wide range of commands to perform a different range of operations, in which it has been splitted between 15 different groups. 
In which the Monix connector only currenly provides support for the most common used ones. in the following 
groups/modules:  ([Keys](https://redis.io/commands#generic), [Hashes](https://redis.io/commands#hash), [List](https://redis.io/commands#list), [Pub/Sub](https://redis.io/commands#pubsub), [Server](https://redis.io/commands#server), [Sets](https://redis.io/commands#set), [SortedSets](https://redis.io/commands#sorted_set), [Streams](https://redis.io/commands#stream) and [Strings](https://redis.io/commands#string)).
Each of these modules has its own object located under `monix.connect.redis`, being `Redis` the one that aggregates all of them. But they can be individually used too.

The only thing you need to do for start using it is to have an implicit `StatefulRedisConnection[K, V]` in the scope. 
 
 See below a complete demonstration on how to compose a different set of Redis command from different 
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
    _ <- Redis.flushall()                 //removes all keys
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
---
### S3
The object storage service that offers industry leading scalability, availability, security and performance.
It allows data storage of any amount of data, commonly used as a data lake for big data applications which can now be easily integrated with monix.
 
 The module has been implemented using the `S3AsyncClient` since it only exposes non blocking methods. 
 Therefore, all of the monix s3 methods defined in the `S3` object would expect an implicit instance of 
 this class to be in the scope of the call.
  
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
---

## Contributing


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

