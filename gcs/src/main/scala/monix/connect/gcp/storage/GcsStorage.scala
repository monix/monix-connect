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

import java.io.FileInputStream
import java.nio.file.Path

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.WriteChannel
import com.google.cloud.storage.Storage._
import com.google.cloud.storage.{BlobId, BlobInfo, Storage, StorageOptions}
import monix.connect.gcp.storage.components.Paging
import monix.connect.gcp.storage.configuration.GcsBucketInfo
import monix.connect.gcp.storage.configuration.GcsBucketInfo.{Locations, Metadata}
import monix.eval.Task
import monix.reactive.Observable

/**
  * This class wraps the [[com.google.cloud.storage.Storage]] class,
  * providing an idiomatic scala API that only exposes side-effectful calls
  * and automatically returns the right types from the monix connect api.
  * @param underlying the google's [[Storage]] instance that this class is based on.
  */
final class GcsStorage(val underlying: Storage) extends Paging {
  self =>

  /** Creates a new [[GcsBucket]] from the give config and options. */
  def createBucket(
    bucketName: String,
    location: Locations.Location,
    metadata: Option[Metadata] = None,
    options: List[BucketTargetOption] = List.empty): Task[GcsBucket] = {
    Task(underlying.create(GcsBucketInfo.withMetadata(bucketName, location, metadata), options: _*))
      .map(GcsBucket.apply)
  }

  /** Creates a new [[GcsBucket]] from the give config and options. */
  def createBlob(bucketName: String, blobName: String, options: List[BlobTargetOption] = List.empty): Task[GcsBlob] = {
    Task {
      val blobInfo = BlobInfo.newBuilder(bucketName, blobName).build()
      underlying.create(blobInfo, options: _*)
    }.map(GcsBlob(_))
  }

  /** Returns the specified bucket as [[GcsStorage]] or [[None]] if it doesn't exist. */
  def getBucket(bucketName: String, options: BucketGetOption*): Task[Option[GcsBucket]] = {
    Task(underlying.get(bucketName, options: _*)).map { bucket => Option(bucket).map(GcsBucket.apply) }
  }

  /** Returns the specified blob as [[GcsBlob]] or [[None]] if it doesn't exist. */
  def getBlob(blobId: BlobId): Task[Option[GcsBlob]] =
    Task(underlying.get(blobId)).map { blob => Option(blob).map(GcsBlob.apply) }

  /** Returns the specified blob as [[GcsBlob]] or [[None]] if it doesn't exist. */
  def getBlob(bucketName: String, blobName: String): Task[Option[GcsBlob]] =
    self.getBlob(BlobId.of(bucketName, blobName))

  /** Returns the specified list of as [[GcsBlob]] or [[None]] if it doesn't exist. */
  def getBlobs(blobIds: List[BlobId]): Task[List[GcsBlob]] =
    Task(blobIds.map(self.getBlob(_))).flatMap { blobIds =>
      Task.sequence(blobIds).map(_.filter(_.isDefined).map(_.get))
    }

  /** Returns an [[Observable]] of all buckets attached to this storage instance. */
  def listBuckets(options: BucketListOption*): Observable[GcsBucket] =
    walk(Task(underlying.list(options: _*))).map(GcsBucket.apply)

  /** Returns an [[Observable]] of all blobs attached to this storage instance. */
  def listBlobs(bucketName: String, options: BlobListOption*): Observable[GcsBlob] =
    walk(Task(underlying.list(bucketName, options: _*))).map(GcsBlob.apply)

}

/** Companion object for [[GcsStorage]] that provides different builder options. */
object GcsStorage {

  def apply(underlying: Storage): GcsStorage =
    new GcsStorage(underlying)

  def create(): GcsStorage =
    new GcsStorage(StorageOptions.getDefaultInstance.getService)

  def create(projectId: String, credentials: Path): GcsStorage =
    new GcsStorage(
      StorageOptions
        .newBuilder()
        .setProjectId(projectId)
        .setCredentials(GoogleCredentials.fromStream(new FileInputStream(credentials.toFile)))
        .build()
        .getService)

}
