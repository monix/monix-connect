/*
 * Copyright (c) 2020-2020 by The Monix Connect Project Developers.
 * See the project homepage at: https://monix.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package monix.connect.gcp.storage

import java.nio.file.Path

import cats.data.NonEmptyList
import com.google.cloud.storage.Bucket.BucketSourceOption
import com.google.cloud.storage.Storage.{BlobGetOption, BlobListOption, BlobWriteOption, BucketTargetOption}
import com.google.cloud.storage.{Acl, BlobId, Bucket}
import monix.connect.gcp.storage.components.{FileIO, GcsDownloader, GcsUploader, Paging}
import monix.connect.gcp.storage.configuration.{GcsBlobInfo, GcsBucketInfo}
import monix.connect.gcp.storage.configuration.GcsBlobInfo.Metadata
import monix.eval.Task
import monix.reactive.Observable

import scala.collection.JavaConverters._

/**
  * This class wraps the [[com.google.cloud.storage.Bucket]] class, providing an idiomatic scala API
  * handling null values with [[Option]] where applicable, as well as wrapping all side-effectful calls
  * in [[monix.eval.Task]] or [[monix.reactive.Observable]].
  */
class GcsBucket private (underlying: Bucket) extends GcsDownloader with FileIO with Paging {
  self =>

  /** Checks if this bucket exists. */
  def exists(options: BucketSourceOption*): Task[Boolean] =
    Task(underlying.exists(options: _*))

  /**
    * Downloads a Blob from GCS, returning an [[Observable]] containing the bytes in chunks of length chunkSize.
    *
    * == Example ==
    *
    * {
    *   import monix.connect.gcp.storage.configuration.GcsBucketInfo.Locations
    *   import monix.connect.gcp.storage.{GcsBucket, GcsStorage}
    *   import monix.eval.Task
    *   import monix.reactive.Observable
    *
    *   val storage = GcsStorage.create()
    *   val bucket: Task[GcsBucket] = storage.createBucket("myBucket", Locations.`EUROPE-WEST3`).memoize
    *
    *   val ob: Observable[Array[Byte]] = for {
    *     bucket <- Observable.fromTask(bucket)
    *     content <- bucket.download("myBlob")
    *   } yield content
    * }
    *
    */
  def download(blobName: String, chunkSize: Int = 4096): Observable[Array[Byte]] = {
    val blobId: BlobId = BlobId.of(underlying.getName, blobName)
    download(underlying.getStorage, blobId, chunkSize)
  }

  /**
    * Allows downloading a Blob from GCS directly to the specified file.
    *
    * == Example ==
    *
    * {
    *   import java.io.File
    *
    *   import monix.connect.gcp.storage.{GcsStorage, GcsBucket}
    *   import monix.eval.Task
    *
    *   val storage: GcsStorage = ???
    *   val getBucketT: Task[Option[GcsBucket]] = storage.getBucket("myBucket")
    *   val targetFile = new File("example/target/file.txt")
    *   val t: Task[Unit] = {
    *     for {
    *       maybeBucket <- getBucketT
    *       _ <- maybeBucket match {
    *         case Some(bucket) => bucket.downloadToFile("myBlob", targetFile.toPath)
    *         case None => Task.unit
    *       }
    *     } yield ()
    *    }
    * }
    */
  def downloadToFile(blobName: String, path: Path, chunkSize: Int = 4096): Task[Unit] = {
    val blobId = BlobId.of(underlying.getName, blobName)
    (for {
      bos   <- openFileOutputStream(path)
      bytes <- download(underlying.getStorage, blobId, chunkSize)
    } yield bos.write(bytes)).completedL
  }

  /**
    * Provides a pre-built [[monix.reactive.Consumer]] implementation from [[GcsUploader]]
    * for uploading data to [[self]] Blob.
    *
    * ==Example==
    *
    * {
    *   import monix.execution.Scheduler.Implicits.global
    *   import monix.connect.gcp.storage.{GcsStorage, GcsBucket}
    *   import monix.connect.gcp.storage.configuration.GcsBucketInfo
    *   import monix.eval.Task
    *   import monix.reactive.Observable
    *
    *   val storage: GcsStorage = ???
    *   val createBucketT: Task[GcsBucket] = storage.createBucket("myBucket", GcsBucketInfo.Locations.`EUROPE-WEST1`).memoize
    *
    *   val ob: Observable[Array[Byte]] = ???
    *   val t: Task[Unit] = for {
    *     bucket <- createBucketT
    *     _ <- ob.consumeWith(bucket.upload("myBlob"))
    *   } yield ()
    *
    *   t.runToFuture(global)
    * }
    */
  def upload(
    name: String,
    metadata: Option[Metadata] = None,
    chunkSize: Int = 4096,
    options: List[BlobWriteOption] = List.empty[BlobWriteOption]): GcsUploader = {
    val blobInfo = GcsBlobInfo.withMetadata(underlying.getName, name, metadata)
    GcsUploader(self.getStorage, blobInfo, chunkSize, options)
  }

  /**
    * Uploads the provided file to the specified target Blob.
    *
    * ==Example==
    *
    * {
    *   import java.io.File
    *
    *   import monix.execution.Scheduler.Implicits.global
    *   import monix.connect.gcp.storage.{GcsStorage, GcsBucket}
    *   import monix.connect.gcp.storage.configuration.GcsBucketInfo
    *   import monix.eval.Task
    *
    *   val storage: GcsStorage = ???
    *   val createBucketT: Task[GcsBucket] = storage.createBucket("myBucket", GcsBucketInfo.Locations.`US-WEST1`)
    *   val sourceFile = new File("example/source/file.txt")
    *
    *   val t: Task[Unit] = for {
    *     bucket <- createBucketT
    *     unit <- bucket.uploadFromFile("myBlob", sourceFile.toPath)
    *   } yield ()
    *
    *  t.runToFuture(global)
    * }
    */
  def uploadFromFile(
    blobName: String,
    path: Path,
    metadata: Option[GcsBlobInfo.Metadata] = None,
    chunkSize: Int = 4096,
    options: List[BlobWriteOption] = List.empty[BlobWriteOption]): Task[Unit] = {
    val blobInfo = GcsBlobInfo.withMetadata(underlying.getName, blobName, metadata)
    openFileInputStream(path).use { bis =>
      Observable
        .fromInputStreamUnsafe(bis)
        .consumeWith(GcsUploader(self.getStorage, blobInfo, chunkSize, options))
    }
  }

  /** Reloads and refreshes this buckets data, returning a new Bucket instance. */
  def reload(options: BucketSourceOption*): Task[Option[GcsBucket]] =
    Task(underlying.reload(options: _*)).map { optBucket => Option(optBucket).map(GcsBucket.apply) }

  /**
    * Updates this bucket with the provided options, returning the newly updated
    * Bucket instance.
    *
    * By default no checks are made on the metadata generation of the current bucket. If
    * you want to update the information only if the current bucket metadata are at their latest
    * version use the [[BucketTargetOption.metagenerationMatch]] option.
    */
  def update(options: BucketTargetOption*): Task[GcsBucket] =
    Task(underlying.update(options: _*))
      .map(GcsBucket.apply)

  /** Deletes this bucket. */
  def delete(options: BucketSourceOption*): Task[Boolean] =
    Task(underlying.delete(options: _*))

  /** Returns the requested blob in this bucket or None if it isn't found. */
  def getBlob(name: String, options: BlobGetOption*): Task[Option[GcsBlob]] = {
    Task(underlying.get(name, options: _*)).map { optBlob => Option(optBlob).map(GcsBlob.apply) }
  }

  /**
    * Returns an [[Observable]] of the requested blobs, if one doesn't exist null is
    * returned and filtered out of the result set.
    */
  def getBlobs(names: NonEmptyList[String]): Observable[GcsBlob] =
    Observable.suspend {
      Observable
        .fromIterable(underlying.get(names.toList.asJava).asScala)
        .filter(_ != null)
        .map(GcsBlob.apply)
    }

  /**
    * Returns a [[Observable]] of the blobs in this [[GcsBucket]]
    * that matched with the passed [[BlobListOption]]s.
    */
  def listBlobs(options: BlobListOption*): Observable[GcsBlob] = {
    walk(Task(underlying.list(options: _*))).map(GcsBlob.apply)
  }

  /** Creates a new ACL entry on this bucket. */
  def createAcl(acl: Acl): Task[Acl] =
    Task(underlying.createAcl(acl))

  /** Returns the ACL entry for the specified entity on this bucket or [[None]] if not found. */
  def getAcl(acl: Acl.Entity): Task[Option[Acl]] =
    Task(underlying.getAcl(acl)).map(Option(_))

  /** Updates an ACL entry on this bucket. */
  def updateAcl(acl: Acl): Task[Acl] =
    Task(underlying.updateAcl(acl))

  /** Deletes the ACL entry for the specified entity on this bucket. */
  def deleteAcl(acl: Acl.Entity): Task[Boolean] =
    Task(underlying.deleteAcl(acl))

  /** Returns a [[Observable]] of all the ACL Entries for this [[GcsBucket]]. */
  def listAcls(): Observable[Acl] =
    Observable.suspend {
      Observable.fromIterable(underlying.listAcls().asScala)
    }

  /**
    * Creates a new default blob ACL entry on this bucket.
    * Default ACLs are applied to a new blob within the bucket when no ACL was provided for that blob.
    */
  def createDefaultAcl(acl: Acl): Task[Acl] =
    Task(underlying.createDefaultAcl(acl))

  /**
    * Returns the default object ACL entry for the specified entity on this bucket
    * or [[None]] if not found.
    */
  def getDefaultAcl(acl: Acl.Entity): Task[Option[Acl]] =
    Task(underlying.getDefaultAcl(acl)).map(Option(_))

  /** Updates a default blob ACL entry on this bucket. */
  def updateDefaultAcl(acl: Acl): Task[Acl] =
    Task(underlying.updateDefaultAcl(acl))

  /** Deletes the default object ACL entry for the specified entity on this bucket. */
  def deleteDefaultAcl(acl: Acl.Entity): Task[Boolean] =
    Task(underlying.deleteDefaultAcl(acl))

  /** Returns a [[Observable]] of all the default Blob ACL Entries for this [[GcsBucket]]. */
  def listDefaultAcls(): Observable[Acl] =
    Observable.suspend {
      Observable.fromIterable(underlying.listDefaultAcls().asScala)
    }

  /** Locks bucket retention policy. Requires a local metageneration value in the request. */
  def lockRetentionPolicy(options: BucketTargetOption*): Task[GcsBucket] =
    Task(underlying.lockRetentionPolicy(options: _*)).map(GcsBucket.apply)

  /** Returns the blob's [[GcsStorage]] object used to issue requests. */
  def getStorage: GcsStorage = GcsStorage(underlying.getStorage)

  /** Returns the blob's [[GcsStorage]] object used to issue requests. */
  def bucketInfo: GcsBucketInfo = GcsBucketInfo.fromJava(underlying)

}

/** Object companion object for [[GcsBucket]]. */
object GcsBucket {
  def apply(bucket: Bucket): GcsBucket = {
    new GcsBucket(bucket)
  }
}
