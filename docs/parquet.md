---
id: parquet
title: Apache Parquet
---

## Introduction
[Apache Parquet](http://parquet.apache.org/) is a columnar storage format that provides the advantages of compressed, efficient data representation available to any project in the Hadoop ecosystem.

 It has already been proved by multiple projects that have demonstrated the performance impact of applying the right compression and encoding scheme to the data.
  
Therefore, the parquet connector basically exposes stream integrations for reading and writing into and from parquet files either in the _local system_, _hdfs_ or _S3_.
 
## Set up

Add the following dependency:
 
 ```scala
 libraryDependencies += "io.monix" %% "monix-parquet" % "0.1.0"
 ```

## Getting started

These two signatures `Parquet.write` and `Parquet.read` are built on top of the _apache parquet_  `ParquetWriter[T]` and `ParquetReader[T]`, therefore they need an instance of these types to be passed.

The below example shows how to construct a parquet consumer that expects _Protobuf_ messages and pushes 
them into the same parquet file of the specified location.

### Writer

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

### Reader

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

## Local testing

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
