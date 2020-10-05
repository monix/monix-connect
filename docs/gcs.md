---
id: gcs
title: Google Cloud Storage
---

## Introduction

_Google Cloud Storage_ is a durable and highly available object storage
service, almost infinitely scalable and 
guarantees consistency: when a write succeeds, the latest copy of the
object will be returned to any _get request_, globally.

## Dependency

Add the following dependency to get started:
```scala
libraryDependencies += "io.monix" %% "monix-gcs" % "0.3.3"
```

## Getting Started

The _Monix Google Cloud Storage_ connector is built on top of the
[Google Cloud Storage Client for Java](https://github.com/googleapis/java-storage) and is divided into three main abstractions: [Storage](https://googleapis.dev/java/google-cloud-storage/latest/index.html), [Bucket](https://googleapis.dev/java/google-cloud-storage/latest/index.html) and [Blob](https://googleapis.dev/java/google-cloud-storage/latest/index.html),
 which will be described and explained in detail on the next sections:
 
### Storage
 
 The ´Storage´ acts as an interface for _Google Cloud Storage_, it provides very basic functionality limited to authentication for creating a connection with the service, 
 and to creating and accessing the _Buckets_ and _Blobs_.

#### Connection

The connector uses the *Application Default Credentials* method for
authenticating to _GCS_. This requires the user to have the
`GOOGLE_APPLICATION_CREDENTIALS` environment variable pointing to a
Service Account with the required permissions in order to use the
connector.

```scala
import monix.connect.gcp.storage.GcsStorage

val storage = GcsStorage.create()
```

Alternatively you will be able to point a credentials
file on disk in the event you don't have the
`GOOGLE_APPLICATION_CREDENTIALS` environment variable set.

```scala
import java.nio.file.Paths

import monix.connect.gcp.storage.GcsStorage

val projectId = "monix-connect-gcs"
val credentials = Paths.get("/path/to/credentials.json")

val storage = GcsStorage.create(projectId, credentials)
```
Once you have a _GcsStorage_ object created you can begin to work with
_GCS_, the first thing is to create a new _GcsBucket_ from the same instance:

#### Create resources

```scala
import java.io.File

import monix.connect.gcp.storage.{GcsStorage, GcsBucket}
import monix.connect.gcp.storage.configuration.GcsBucketInfo
import monix.connect.gcp.storage.configuration.GcsBucketInfo.Locations

val storage = GcsStorage.create()

val metadata = GcsBucketInfo.Metadata(
  labels = Map(
    "project" -> "my-first-gcs-bucket"
  ),
  storageClass = Some(StorageClass.REGIONAL)
)
val bucket: Task[GcsBucket] = storage.createBucket("mybucket", Locations.`EUROPE-WEST1`, Some(metadata)).memoizeOnSuccess
```

On the other hand, you can create _Blobs_ in the same way as with _Bucket_.

```scala
import monix.connect.gcp.storage.{GcsStorage, GcsBlob}

val storage: GcsStorage = GcsStorage.create()
val blob: Task[GcsBlob] = storage.createBlob("mybucket", "myBlob").memoizeOnSuccess
```

It also exposes a get operation for _buckets_ and _blobs_ that gets executed `asyncronously` and it is `type-safe`, returning an _Option_
with the resource we asked for, being _None_ if it did not existed:

```scala
val t: Task[Unit] = {
  for {
    maybeBucket <- storage.getBucket("myBucket"): Task[Option[GcsBucket]]
    _ <- maybeBucket match {
      case Some(bucket) => Task.now(println("My bucket exists!"))
      case None => Task.unit // alternatively a failure could be raised
    }
  } yield ()
}
```

The same would apply for _Blob_.

```scala
val getBlobT:  = 
val t: Task[Unit] = {
  for {
    maybeBlob <- storage.getBlob("myBucket", "myBlob"): Task[Option[GcsBlob]]
    _ <- maybeBlob match {
      case Some(blob) => Task.now(println("My blob exists!"))
      case None => Task.unit // alternatively a failure could be raised

    }
  } yield ()
}
```

You could also find a list of _buckets_ or _blobs_ by using respectively the signatures `getBuckets` and `getBlobs`, and also list all of them with `listBuckets` and `listBlobs`.

### Buckets

A [Bucket](https://cloud.google.com/storage/docs/key-terms#buckets) is basically a container that holds your data in _GCS_. 
You can use _buckets_ to organize your data and control its access but unlike directories and folders, you cannot nest them. 

The _Monix GCS_ connector relies in the underlying `com.google.cloud.storage.Bucket`, but with some additions and and integrations with `Monix` data types that makes it possible expose an idiomatic and `type-safe` non blocking api. 

This implementation is named `GcsBucket`, and you can start using it different ways listed in the following example:

```scala
import java.io.File

import monix.connect.gcp.storage.{GcsStorage, GcsBucket}

val storage: GcsStorage = GcsStorage.create()

/** 1- When creating a bucket you will make sure that the bucket you want to use exists,
  * since it returns the new bucket on completion. */
val bucket1: Task[GcsBucket] = storage.createBucket("mybucket1", Locations.`EUROPE-WEST1`).memoizeOnSuccess

/** 2- You can also get / find the bucket by its name, in this case if it does not exist
  * it will return an empty Option. */
val bucket2: Task[Option[GcsBucket]] = storage.getBucket("myBucket2")

/** 3- Finally, if you do already have an instance of [[com.google.cloud.storage.Bucket]],
  * you can convert it to a GcsBucket by using its compainon object*/
// val bucket3: GcsBucket = GcsBucket(underlying)
```

Once we have an instance of `GcsBucket`, we will be able to use its very simple methods that it exposes to manage our _Bucket_, such like _get blob/s_ stored in it, _update_, _reload_ its metadata, various ones to manage its _Access Control List_ (_ACL_), etc.

There are no code examples on the documentation to show these operations since they are very basic and easy to use. 
 On the other hand, there are also available methods for _uploading_ and _downloading_ from _Blobs_ of this same _Bucket_, they are very interesting and unique from this connector, see how can they be used in below code examples.

#### download

In order to download a blob using the `GcsBucket` you would just need to specify the _Blob_ name that should be allocated in the same _Bucket_:

```scala
val bucket: Task[Option[GcsBucket]] = storage.getBucket("myBucket")
val ob: Observable[Array[Byte]] = {
  Observable.fromTask(bucket)
    .flatMap {
      case Some(blob) => blob.download("myBlob")
      case None => Observable.empty // alternatively a failure can be raised
    }
}
```

##### downloadToFile

There is also a handy operation for downloading directly into a file, 
beware that _GCS_ is designed to allocate files of any size, 
therefore it should only be used with relative small files that we know for a fact that our local system have enough capacity. 

```scala
import java.io.File

import monix.connect.gcp.storage.{GcsStorage, GcsBucket}
import monix.eval.Task

val storage = GcsStorage.create()
val targetFile = new File("example/target/file.txt")

val t: Task[Unit] = {
  for {
    maybeBucket <- storage.getBucket("myBucket"): Task[Option[GcsBucket]]
    _ <- maybeBucket match {
      case Some(bucket) => bucket.downloadToFile("myBlob", targetFile.toPath)
      case None => Task.unit // alternatively a failure can be raised
    }
  } yield ()
}
```

##### upload

On the other hand you can upload data into a _Blob_ by using the pre-built `Consumer` implementation that expects and pushes _byte arrays_
  into the specified `Blob` and materializes to `Unit` when it completes.   

```scala

import monix.connect.gcp.storage.{GcsStorage, GcsBucket}
import monix.eval.Task

val storage = GcsStorage.create()

val content = "Dummy content"
val ob: Observable[Array[Byte]] = Observable.now(content.getBytes)

val t: Task[Unit] = for {
 bucket <- storage.createBucket("myBucket", Locations.`EUROPE-WEST1`).memoize
  _ <- ob.consumeWith(b.upload("myBlob"))
} yield ()
```

##### uploadFromFile

Alternatively, you can upload data from a local file into the specified _Blob_. 

```scala
import java.io.File

import monix.connect.gcp.storage.{GcsStorage, GcsBucket}
import monix.eval.Task

val bucketT: Task[GcsBucket] = ???
val sourceFile = new File("example/source/file.txt")

val t: Task[Unit] = for {
  b <- bucketT
  unit <- b.uploadFromFile("myBlob", sourceFile.toPath)
} yield ()
```

### Blobs

A _Blob_ is nothing else than an _Object_, pieces of data that you have uploaded to Cloud Storage that have to reside in a _Bucket_.

The representation of an object in this connector is called `GcsBlob`, it provides also various simple methods for managing
things like _update_ metadata, manage its _acl_ and _delete_ permanently. 

`GcsBlob` also exposes the methods _download_, _downloadToFile_, _upload_ and _uploadFromFile_ that allow to manage the _Blob's_ data in a reactive way.
The only difference from using `GcsBucket` is that but in this case there is no need of specifying the _Bucket_, 
since it will use the one which the _Blob_ is stored in.

##### copyTo 

This thod has different signatures, and it allows you to copy a _Blob_ into the specified target _Bucket_ and _Blob_.
The target _Bucket_ can be the same or a different as the source.

```scala
import monix.connect.gcp.storage.{GcsStorage, GcsBlob}
import monix.eval.Task
val storage = GcsStorage.create()
val sourceBlob: Task[GcsBlob] = storage.createBlob("myBucket", "sourceBlob")
val targetBlob: Task[GcsBlob] =  sourceBlob.flatMap(_.copyTo("targetBucket", "targetBlob"))
```

## Local testing

Testing _Google Cloud Storage_ locally and offline is challenging since there is yet _'not too good support'_ on that front.

There is a google library called [java-storage-nio](https://github.com/googleapis/java-storage-nio) that emulates this service,
 however, it has some limitations since it does [not provide support](https://github.com/vam-google/google-cloud-java/blob/b095221d438f3b1c3b0929d9ab064be6051c2ba2/google-cloud-contrib/google-cloud-nio/src/main/java/com/google/cloud/storage/contrib/nio/testing/LocalStorageHelper.java#L27)
  for some the operations (mostly for the _Bucket_ api) and it is not _thread-safe_.
  That's why it is highly recommended to run the _functional tests_ directly using the _Google Cloud Storage_ service.
  
However, in case you can not access to the real google cloud service, this library will be suitable for you:
 
 Add it to the _sbt library dependencies_:
 ```scala
 libraryDependencies ++= "com.google.cloud" % "google-cloud-nio" % "0.121.2" % Test
 ```

Then you should be able to create a _fake_ `Storage` instance and use it to build `GcsStorage` from the companion object
_apply_ method. 

```scala
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import com.google.cloud.storage.{Blob, BlobId, BlobInfo, Storage, Option => _}

val storage: Storage = LocalStorageHelper.getOptions.getService
val blobInfo: BlobInfo = BlobInfo.newBuilder(BlobId.of("myBucket", "myBlob")).build
val blob: Blob = storage.create(blobInfo)
val gcsBlob: GcsBlob = new GcsBlob(blob)
```

Some advantages against using the real service would be that it does not require to deal with any type of _google access credentials_, 
which may be good in some cases and it can save crucial time spent on setting the right credentials.








