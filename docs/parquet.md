---
id: parquet
title: Apache Parquet
---

## Introduction
[Apache Parquet](http://parquet.apache.org/) is a columnar storage format that provides the advantages of compressed, efficient data representation available to any project in the _Hadoop_ ecosystem.

 It has already been proved by multiple projects that have demonstrated the performance impact of applying the right compression and encoding scheme to the data.
  
Therefore, the `monix-parquet` _connector_ basically exposes stream integrations for _reading_ and _writing_ into and from parquet files either in the _local system_, _hdfs_ or _S3_ (this is at least for `avro` and `protobuf` parquet sub-modules).
 
## Set up

Add the following dependency:
 
 ```scala
 libraryDependencies += "io.monix" %% "monix-parquet" % "0.1.0"
 ```

## Getting started

These two signatures `write` and `read` are built on top of the _apache parquet_  `ParquetWriter[T]` and `ParquetReader[T]` respectively, therefore they need an instance of these types to be passed.

The _type parameter_ `T` represents the data type that is expected to be read or written in the parquet file.
In which it can depend on the parquet implementation chosen, since `ParqueReader` and `ParquetWriter` are just the 
 generic classes, but you would need to use the implementation that fits to your use case.

  
### Writer

The below example shows how to construct a parquet consumer that expects _Protobuf_ messages and pushes 
them into the same parquet file of the specified location.
In this case, the type parameter `T` would need to implement `com.google.protobuf.Message`, 
and these were generated using [ScalaPB](https://scalapb.github.io/).

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

```

In case you are seeking an example of avro parquet writer, you can refer to the [avro parquet tests](/parquet/src/test/scala/monix/connect/parquet/AvroParquetSpec.scala). 

### Reader

On the other hand, the following code shows how to pull _Avro_ records from a parquet file.
In contrast, this time the type parameter `T` would need to be a subtype of `org.apache.avro.generic.GenericRecord`, 
In this case we used  is `com.sksamuel.avro4s.Record` generated using [Avro4s](https://github.com/sksamuel/avro4s),
 but there are other libraries there such as [Avrohugger](https://github.com/julianpeeters/avrohugger) to generate these classes.

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

```

_Warning_: This connector provides with the logic of building a publisher and subscriber from a given apache hadoop `ParquetReader` and `ParquetWriter` respectively,
but it does not cover any existing issue on the support of the apache parquet library with external ones.
Notice for example, that an [issue](https://github.com/scalapb/ScalaPB/issues/844) was found while writing tests for _reading_ parquet as protobuf messages generated with `SacalaPB`, on the other hand for _writing_ it was all fine. 

## Local testing

It will depend on the specific use case, as we mentioned earlier in the introductory section it can operate on the _local filesystem_ on _hdfs_ or even in _S3_ (for _avro_ and _protobuf_ parquet sub-modules)

Therefore, depending on the application requirements, the hadoop `Configuration` class will need to be configured accordingly.
 
__Local:__ So far in the examples has been shown how to use it locally, in which in that case it would just be needed to create a plain instance of hadoop configuration, and the `Path` that would 
 represent the file in the local system. 

__Hdfs:__ On the other hand, the most common case is to work with parquet files in hdfs, in that case my recommendation is to find specific posts and examples on how to set up your configuration for that.
But on some extend, for setting up the local test environment you would need to use the hadoop minicluster and set the configuration accordingly. 
You can check the how to do so in the `monix-hdfs` documentation. 

__S3:__ Finally, integrating the parequet connector with AWS S3 requires [specific configuration values](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html) to be set. On behalf of configuring it 
to run local tests ... 
Note that you will also require to spin up a docker container for emulating the AWS S3 service, check how to do so in the `monix-s3` documentation. 
