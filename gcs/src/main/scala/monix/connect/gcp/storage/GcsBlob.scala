/*
 * Copyright (c) 2020-2020 by The Monix Connect Project Developers.
 * See the project homepage at: https://connect.monix.io
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

import java.net.URL
import java.nio.file.Path

import com.google.cloud.storage.Blob.BlobSourceOption
import com.google.cloud.storage.Storage.{BlobTargetOption, BlobWriteOption, SignUrlOption}
import com.google.cloud.storage.{Acl, Blob, BlobId}
import com.google.cloud.{storage => google}
import monix.connect.gcp.storage.components.{FileIO, GcsDownloader, GcsUploader}
import monix.connect.gcp.storage.configuration.GcsBlobInfo
import monix.eval.Task
import monix.reactive.Observable

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

/**
  * This class wraps the [[com.google.cloud.storage.Blob]] class, providing an idiomatic scala API
  * handling null values with [[Option]] where applicable, as well as wrapping all side-effectful calls
  * in [[monix.eval.Task]] or [[monix.reactive.Observable]].
  *
  * @define copyToNote Forcing an Async Boundary, this function potentially spins until the copy is done. If the src and
  *                    dst are in the same location and share the same storage class the request is done in one RPC call,
  *                    otherwise multiple calls are issued.
  */
private[storage] final class GcsBlob(val underlying: Blob) extends GcsDownloader with FileIO {
  self =>

  /**
    * Downloads a Blob from GCS, returning an Observable containing the bytes in chunks of length chunkSize.
    *
    * == Example ==
    *
    * {{{
    *   import monix.connect.gcp.storage.{GcsStorage, GcsBlob}
    *   import monix.eval.Task
    *   import monix.reactive.Observable
    *
    *    val storage = GcsStorage.create()
    *    val memoizedBlob = storage.getBlob("myBucket", "myBlob").memoize
    *
    *    val ob: Observable[Array[Byte]] = {
    *      for {
    *        blob <- Observable.fromTask(memoizedBlob): Observable[Option[GcsBlob]]
    *        bytes <- {
    *          blob match {
    *            case Some(blob) => blob.download()
    *            case None => Observable.empty
    *          }
    *        }
    *      } yield bytes
    *    }
    * }}}
    *
    */
  def download(chunkSize: Int = 4096): Observable[Array[Byte]] = {
    val blobId: BlobId = BlobId.of(underlying.getBucket, underlying.getName)
    download(underlying.getStorage, blobId, chunkSize)
  }

  /**
    * Allows downloading a Blob from GCS directly to the specified file.
    *
    * == Example ==
    *
    * {{{
    *   import java.io.File
    *
    *   import monix.execution.Scheduler.Implicits.global
    *   import monix.connect.gcp.storage.{GcsStorage, GcsBlob}
    *   import monix.eval.Task
    *
    *   val storage: GcsStorage = GcsStorage.create()
    *   val file = new File("path/to/your/path.txt")
    *
    *   val t: Task[Unit] = {
    *     for {
    *       maybeBlob <- storage.getBlob("myBucket", "myBlob"): Task[Option[GcsBlob]]
    *       _ <- maybeBlob match {
    *         case Some(blob) => blob.downloadToFile(file.toPath)
    *         case None => Task.unit // alternatively a failure can be raised
    *       }
    *     } yield ()
    *   }
    * }}}
    */
  def downloadToFile(path: Path, chunkSize: Int = 4096): Task[Unit] = {
    val blobId: BlobId = BlobId.of(underlying.getBucket, underlying.getName)
    (for {
      bos   <- openFileOutputStream(path)
      bytes <- download(underlying.getStorage, blobId, chunkSize)
    } yield bos.write(bytes)).completedL
  }

  /** Checks if this blob exists. */
  def exists(options: BlobSourceOption*): Task[Boolean] =
    Task(underlying.exists(options: _*))

  /** Fetches current blob's latest information. Returns None if the blob does not exist. */
  def reload(options: BlobSourceOption*): Task[Option[GcsBlob]] = {
    Task(underlying.reload(options: _*)).map { optBlob => Option(optBlob).map(GcsBlob.apply) }
  }

  /**
    * Uploads the provided file to the specified target Blob.
    *
    * == Example ==
    *
    * {{{
    *   import monix.connect.gcp.storage.{GcsStorage, GcsBlob}
    *   import monix.eval.Task
    *
    *   import java.io.File
    *
    *   val storage: GcsStorage = GcsStorage.create()
    *   val sourceFile = new File("path/to/your/path.txt")
    *
    *   val t: Task[Unit] = for {
    *     blob <- storage.createBlob("myBucket", "myBlob"): Task[GcsBlob]
    *     _ <- blob.uploadFromFile(sourceFile.toPath)
    *   } yield ()
    * }}}
    */
  def uploadFromFile(
    path: Path,
    metadata: Option[GcsBlobInfo.Metadata] = None,
    chunkSize: Int = 4096,
    options: List[BlobWriteOption] = List.empty[BlobWriteOption]): Task[Unit] = {
    val blobInfo = GcsBlobInfo.withMetadata(underlying.getBucket, underlying.getName, metadata)
    openFileInputStream(path).use { bis =>
      Observable
        .fromInputStreamUnsafe(bis)
        .consumeWith(GcsUploader(self.getStorage, blobInfo, chunkSize, options))
    }
  }

  /**
    * A pre-built [[monix.reactive.Consumer]] implementation from [[GcsUploader]]
    * for uploading data to [[self]] Blob.
    *
    * == Example ==
    *
    * {{{
    *   import monix.connect.gcp.storage.{GcsStorage, GcsBlob}
    *   import monix.eval.Task
    *   import monix.reactive.Observable
    *
    *   val storage: GcsStorage = GcsStorage.create()
    *   val memoizedBlob: Task[GcsBlob] = storage.createBlob("myBucket", "myBlob").memoize
    *
    *   val ob: Observable[Array[Byte]] = Observable.now("dummy content".getBytes)
    *
    *   val t: Task[Unit] = for {
    *     blob <- memoizedBlob
    *     _ <- ob.consumeWith(blob.upload())
    *   } yield ()
    * }}}
    */
  def upload(
    metadata: Option[GcsBlobInfo.Metadata] = None,
    chunkSize: Int = 4096,
    options: List[BlobWriteOption] = List.empty[BlobWriteOption]): GcsUploader = {
    val blobInfo = GcsBlobInfo.withMetadata(underlying.getBucket, underlying.getName, metadata)
    GcsUploader(self.getStorage, blobInfo, chunkSize, options)
  }

  /**
    * Updates the blob's information. The Blob's name cannot be changed by this method. If you
    * want to rename the blob or move it to a different bucket use the [[copyTo]] and [[delete]] operations.
    */
  def update(options: BlobTargetOption*): Task[GcsBlob] = {
    Task(underlying.update(options: _*))
      .map(GcsBlob.apply)
  }

  /**
    * Updates the blob's information. Bucket or blob's name cannot be changed by this method. If you
    * want to rename the blob or move it to a different bucket use the [[copyTo]] and [[delete]] operations.
    */
  def updateMetadata(metadata: GcsBlobInfo.Metadata, options: BlobTargetOption*): Task[GcsBlob] = {
    val updated = GcsBlobInfo.withMetadata(underlying.getBucket, underlying.getName, Some(metadata))
    Task(underlying.getStorage.update(updated, options: _*))
      .map(GcsBlob.apply)
  }

  /** Deletes this blob. */
  def delete(options: BlobSourceOption*): Task[Boolean] =
    Task(underlying.delete(options: _*))

  /** Copies this blob to the target Blob. */
  def copyTo(targetBlob: BlobId, options: BlobSourceOption*): Task[GcsBlob] =
    Task
      .evalAsync(underlying.copyTo(targetBlob, options: _*))
      .map(_.getResult)
      .map(GcsBlob.apply)

  /**
    * Copies this blob to the target Bucket.
    */
  def copyTo(targetBucket: String, options: BlobSourceOption*): Task[GcsBlob] =
    Task
      .evalAsync(underlying.copyTo(targetBucket, options: _*))
      .map(_.getResult)
      .map(GcsBlob.apply)

  /**
    * Copies this blob to the target Blob in the target Bucket.
    */
  def copyTo(targetBucket: String, targetBlob: String, options: BlobSourceOption*): Task[GcsBlob] =
    Task
      .evalAsync(underlying.copyTo(targetBucket, targetBlob, options: _*))
      .map(_.getResult)
      .map(GcsBlob.apply)

  /**
    * Generates a signed URL for this blob. If you want to allow access for a fixed amount of time to
    * this blob, you can use this method to generate a URL that is only valid within a certain time
    * period. This is particularly useful if you don't want publicly accessible blobs, but also don't
    * want to require users to explicitly log in. Signing a URL requires a service account signer.
    *
    * If an instance of [[com.google.auth.ServiceAccountSigner]] was passed to [[com.google.cloud.storage.StorageOptions]]
    * builder via [[com.google.cloud.storage.StorageOptions#setCredentials]] or the default credentials are being
    * used and the environment variable 'GOOGLE_APPLICATION_CREDENTIALS' is set or your application is running in
    * App Engine, then this function will use those credentials to sign the URL.
    *
    * If the credentials passed to [[com.google.cloud.storage.StorageOptions]] do not implement
    * [[com.google.auth.ServiceAccountSigner]] (this is the case, for instance, for Compute Engine credentials and
    * Google Cloud SDK credentials) then this function will throw an [[IllegalStateException]] unless an implementation
    * of [[com.google.auth.ServiceAccountSigner]] is passed using the [[SignUrlOption#signWith(ServiceAccountSigner)]]
    * option.
    */
  def signUrl(duration: FiniteDuration, options: SignUrlOption*): Task[URL] =
    Task(underlying.signUrl(duration.length, duration.unit, options: _*))

  /** Creates a new ACL entry on this Blob. */
  def createAcl(acl: Acl): Task[Acl] =
    Task(underlying.createAcl(acl))

  /** Returns the [[Acl]] entry for the specified entity on this [[GcsBlob]] or [[None]] if not found. */
  def getAcl(acl: Acl.Entity): Task[Option[Acl]] =
    Task(underlying.getAcl(acl)).map(Option(_))

  /** Updates an ACL entry on this [[GcsBlob]]. */
  def updateAcl(acl: Acl): Task[Acl] =
    Task(underlying.updateAcl(acl))

  /** Deletes the [[Acl]] entry for the specified [[com.google.cloud.storage.Acl.Entity]] on this [[GcsBlob]]. */
  def deleteAcl(acl: Acl.Entity): Task[Boolean] =
    Task(underlying.deleteAcl(acl))

  /** Returns an [[Observable]] of all the [[Acl]] Entries for this [[GcsBlob]]. */
  def listAcls(): Observable[Acl] =
    Observable.suspend {
      Observable.fromIterable(underlying.listAcls().asScala)
    }

  /** Returns the blob's [[GcsStorage]] object used to issue requests. */
  def getStorage: GcsStorage = GcsStorage(underlying.getStorage)

  /** Returns all the metadata associated with this Blob instance. */
  def blobInfo: GcsBlobInfo = GcsBlobInfo.fromJava(underlying)

}

/* Companion object of GcsBlob */
object GcsBlob {
  def apply(blob: google.Blob) = new GcsBlob(blob)
}
