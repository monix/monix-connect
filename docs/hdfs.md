---
id: hdfs
title: HDFS
---

## Introduction

The _Hadoop Distributed File System_ ([HDFS](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html)) is a distributed file system designed to run on commodity hardware, 
also being highly fault-tolerant and providing high throughput access makes it very suitable for applications that have to handle with large data sets.

This connector then allows to _read_ and _write_ hdfs files of any size in a streaming fashion.

The methods to perform these operations are exposed under the object ```monix.connect.hdfs.Hdfs```, 
and it is built on top of the the official _apache hadoop_ api.  

## Dependency

Add the following dependency to get started:

```scala 
libraryDependencies += "io.monix" %% "monix-hdfs" % "0.6.0-RC1"
```

By default the connector uses _Hadoop 3.1.1_. In case you need a different one you can replace it by excluding `org.apache.hadoop` from `monix-hdfs` and add the new one to your library dependencies.

## Getting started

### Configuration

The following import is a common requirement for all those methods defined in the `Hdfs` object:
```scala
import org.apache.hadoop.fs.FileSystem
//abstract representation of a file system which could be a distributed or a local one.
import org.apache.hadoop.fs.Path
//represents a file or directory in a FileSystem
```

Each use case would need different settings to create the hadoop configurations, but 
 for testing purposes we would just need a plain one: 
 
```scala
import org.apache.hadoop.conf.Configuration

val hadoopConf = new Configuration() //provides access to the hadoop configurable parameters
val fs: FileSystem = FileSystem.get(conf)
```
 
 ### Reader
 
Let's start interacting with hdfs, the following example shows how to construct a pipeline that reads from the specified hdfs file.

Normally working with hdfs means that you will be dealing with big data, it makes it difficult or very expensive (if not impossible) to read the whole file at once from a single machine,
 therefore the application will read the file in small parts configured by the user that eventually will end with the whole file being processed. 

```scala
val sourcePath: Path = new Path("/source/hdfs/file_source.txt")
val chunkSize: Int = 8192 //size of the chunks to be pulled

//once we have the hadoop classes we can create the hdfs monix reader
val ob: Observable[Array[Byte]] = Hdfs.read(fs, path, chunkSize)
```

 ### Writer

The following example shows how consume a stream of bytes create a file and writes chunks into it:
 
 ```scala
val destinationPath: Path = new Path("/destination/hdfs/file_dest.txt")
val hdfsWriter: Consumer[Array[Byte], Task[Long]] = Hdfs.write(fs, destinationPath) 

//eventually it will return the size of the written file
val t: Task[Long] = ob.consumeWith(hdfsWriter) 
 ```
It materializes to a `Long` value that represents the file size.

Note that the hdfs `write` signature allows different configurations to be passed as parameters such as to
_enable overwrite_ (`true` by default), _replication factor_ `3`, the _bufferSize_  of `4096 bytes`, _blockSize_ `134217728 bytes =~ 128 MB`_ 
and finally a _line separator_ which is not used by default _None_.

Below example shows an example on how easily can them be tweaked:

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

 ### Appender

Finally, the hdfs connector also exposes an _append_ operation which is very similar to _writer_ implementation, 
but in this case the materialized `Long` value only represents the size of the appended data, but not of the whole file. 

On the other hand, this method does not allow to configure neither the _replication factor_ nor _block size_ and so on, this is because
these configurations are only set whenever a file is created but an append operation would reuse them from the existing file.

See below an example:

```scala
val hadoopConf = new Configuration() 
//enables the append operation
hadoopConf.setBoolean("dfs.support.append", true)
//found it necessary when running tests on hadoop mini-cluster, but you should tweak the hadoopConf accordingly to your use case
hadoopConf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER") 
hadoopConf.set("fs.default.name", s"hdfs://localhost:$port") //especifies the local endpoint of the test hadoop minicluster
val fs: FileSystem = FileSystem.get(conf)

//note that we are re-using the `destinationPath` of the last example since should already exist
val hdfsAppender: Consumer[Array[Byte], Task[Long]] = Hdfs.append(fs, destinationPath) 
val ob: Observer[Array[Byte]] = ???
val t: Task[Long] = ob.consumeWith(hdfsAppender) 
```
 
## Local testing
 
 _Apache Hadoop_ has a sub project called [Mini Cluster](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-minicluster) 
 that allows to locally spin up a single-node Hadoop cluster without the need to set any environment variables or manage hadoop configuration files.
  
Add to your library dependencies with the desired version:
 
```scala
"org.apache.hadoop" % "hadoop-minicluster" % "VERSION" % Test
```

From there on, as in this case the tests won't depend on a docker container but will depend the emulation running in the JVM, 
you will have to start and stop the hadoop mini cluster from the same test, as a good practice using `BeforeAndAfterAll`:

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
