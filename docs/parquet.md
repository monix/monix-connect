---
id: parquet
title: Apache Parquet
---

## Introduction
[Apache Parquet](http://parquet.apache.org/) is a columnar storage format that provides the advantages of _compressed_, _efficient_ data representation available to any project in the _Hadoop_ ecosystem.

It has already been proved by multiple projects that have demonstrated the performance impact of applying the right _compression_ and _encoding scheme_ to the data.
  
Therefore, the `monix-parquet` _connector_ basically exposes stream integrations for _reading_ and _writing_  from and into parquet files either in the _local system_, _hdfs_ or _AWS S3_.
 
## Set up

Add the following dependency:
 
 ```scala
 libraryDependencies += "io.monix" %% "monix-parquet" % "0.5.1"
 ```

## Getting started

The two signatures `write` and `read` are built on top of the _apache parquet_ `ParquetWriter[T]` and `ParquetReader[T]` respectively, therefore they need an instance of these types to be passed.

The _type parameter_ `T` represents the data type that is expected to be read or written in the parquet file.
In which it can depend on the parquet implementation chosen, since `ParqueReader` and `ParquetWriter` are just the 
 generic classes but you would need to use the implementation that fits better to your use case.

The examples shown in the following sections uses the subclass `AvroParquet` in which the type parameter `T` would need to be a subtype of `org.apache.avro.generic.GenericRecord`. 
For these examples we have created our own schema using `org.apache.avro.Schema`, however, it is *highly recommended* to generate them using  one of the existing libraries in the scala ecosystem such like [Avro4s](https://github.com/sksamuel/avro4s), [Avrohugger](https://github.com/julianpeeters/avrohugger), [Vulcan](https://fd4s.github.io/vulcan/docs/modules).

 ```scala
// used to parse from and to `GenericRecord`, in a real world example it would be a subtype of `GenericRecord`.
case class Person(id: Int, name: String)

import org.apache.avro.Schema
// avro schema associated to the above case class 
val schema: Schema = new Schema.Parser().parse(
    "{\"type\":\"record\",\"name\":\"Person\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}]}")
```

### Writer

Now, let's get our hands dirty with an example on how to write from `Person` into a parquet file.
 To do so, we are going to use `AvroParquetWriter` which expects elements subtype of `GenericRecord`, 
 First, we we need a function to convert from our `Person`s: 
 
```scala
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}

// wouldn't be necessary if we were using a scala avro schema generator (avro4s, avrohugger, ...)
def personToGenericRecord(person: Person): GenericRecord =
    new GenericRecordBuilder(schema) // using the schema created previously
      .set("id", person.id)
      .set("name", person.name)
      .build()
```

And then we can consume an `Observable[Person]` that will write each of the emitted elements into the specified parquet file.
 
```scala
import monix.eval.Task
import monix.reactive.Observable
import monix.connect.parquet.ParquetSink
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration

val conf = new Configuration()
val path: Path = new Path("./writer/example/file.parquet")
val elements: List[Person] = List(Person(1, "Alice"), Person(2, "Bob")) 
val parquetWriter: ParquetWriter[GenericRecord] = {
  AvroParquetWriter.builder[GenericRecord](path)
    .withConf(conf)
    .withSchema(schema)
    .build()
}

// returns the number of written records
val t: Task[Long] = {
  Observable
    .fromIterable(elements) // Observable[Person]
    .map(_ => personToGenericRecord(_))
    .consumeWith(ParquetSink.fromWriterUnsafe(parquetWriter))
}
```

### Reader

Again, since we are using a low level api we need to write a function to to convert from `GenericRecord` to a `Person`:

```scala
def recordToPerson(record: GenericRecord): Person =
    Person(record.get("id").asInstanceOf[Int], 
           record.get("name").toString)
```

Then we will be able to read _parquet_ files as `Observable[Person]`. 

```scala
import monix.connect.parquet.ParquetSource
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroParquetReader, AvroReadSupport}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.util.HadoopInputFile

val conf = new Configuration()
conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true)
val path: Path = new Path("./reader/example/file.parquet")
val reader: ParquetReader[GenericRecord] = {
 AvroParquetReader
  .builder[GenericRecord](HadoopInputFile.fromPath(path, conf))
  .withConf(conf)
  .build()
}

val ob: Observable[Person] = ParquetSource.fromReaderUnsafe(reader).map(recordToPerson)
```

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
