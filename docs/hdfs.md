---
id: hdfs
title: HDFS
---

## Introduction

The _Hadoop Distributed File System_ ([HDFS](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html)) is a distributed file system designed to run on commodity hardware, 
it is is highly fault-tolerant and it provides high throughput access which makes it suitable for applications that have to handle with large data sets.

This connector then allows to read and write hdfs files of any size in a streaming fashion.

The methods to perform these operations are exposed under the object ```monix.connect.hdfs.Hdfs```, in which
it has been built on top of the the official _apache hadoop_ api.  


## Dependency

Add the following dependency to get started:
```scala 
libraryDependencies += "io.monix" %% "monix-hdfs" % "0.1.0"
```

By default the connector uses Hadoop version 3.1.1. In case you need a different one you can replace it by excluding `org.apache.hadoop` from `monix-hdfs` and add the new one to your library dependencies.


## Getting started

### Configuration

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
 
 ### Reader

Then we can start interacting with hdfs, the following example shows how to construct a pipeline that reads from the specified hdfs file.

```scala

val sourcePath: Path = new Path("/source/hdfs/file_source.txt")
val chunkSize: Int = 8192 //size of the chunks to be pulled

//Once we have the hadoop classes we can create the hdfs monix reader
val ob: Observable[Array[Byte]] = Hdfs.read(fs, path, chunkSize)
```

Since using hdfs means we are dealing with big data, it makes it difficult or very expensive to read the whole file at once,
 therefore with the above example we will read the file in small parts configured by `chunkSize` that eventually will end with the whole file being processed. 

 ### Writer

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

 ### Appender

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
 
## Local testing
 
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
