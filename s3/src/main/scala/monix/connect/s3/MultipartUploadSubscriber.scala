/*
 * Copyright (c) 2020-2021 by The Monix Connect Project Developers.
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

package monix.connect.s3

import monix.catnap.MVar
import monix.connect.s3.domain.UploadSettings
import monix.eval.Task
import monix.execution.cancelables.AssignableCancelable
import monix.execution.internal.InternalApi
import monix.execution.{Ack, Callback, Scheduler}
import monix.reactive.observers.Subscriber
import monix.reactive.Consumer
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{
  CompleteMultipartUploadResponse,
  CompletedPart,
  CreateMultipartUploadRequest
}

import scala.concurrent.Future
import scala.util.control.NonFatal

/**
  * The multipart upload subscriber allows to upload large objects in parts.
  * Multipart uploading is a three-step process:
  * 1- Initialised the upload
  * 2- Upload the object parts
  * 3- And after you have uploaded all the parts, you complete the multipart upload.
  * Upon receiving the complete multipart upload request,
  * The object is constructed from the different uploaded parts, and you can
  * then access the object just as you would any other object in your bucket.
  */
@InternalApi
private[s3] class MultipartUploadSubscriber(
  bucket: String,
  key: String,
  minChunkSize: Int,
  uploadSettings: UploadSettings,
  s3Client: S3AsyncClient)
  extends Consumer[Array[Byte], CompleteMultipartUploadResponse] {

  def createSubscriber(
    callback: Callback[Throwable, CompleteMultipartUploadResponse],
    s: Scheduler): (Subscriber[Array[Byte]], AssignableCancelable) = {
    val out = new Subscriber[Array[Byte]] {

      implicit val scheduler = s
      private[this] val createRequest: CreateMultipartUploadRequest =
        S3RequestBuilder.createMultipartUploadRequest(bucket, key, uploadSettings)
      // initialises the multipart upload
      private[this] val uploadId: Task[String] =
        Task
          .from(s3Client.createMultipartUpload(createRequest))
          .map(_.uploadId())
          .memoize

      if (minChunkSize < domain.awsMinChunkSize) {
        callback.onError(new IllegalArgumentException("minChunkSize can not be smaller than 5MB"))
      }

      private[this] var buffer: Array[Byte] = Array.emptyByteArray
      private[this] var completedParts: List[CompletedPart] = List.empty[CompletedPart]
      private[this] val partNMVarEval: Task[MVar[Task, Int]] = MVar[Task].of(1).memoize

      /**
        * If the chunk length is bigger than the minimum size, perfom the part request [[software.amazon.awssdk.services.s3.model.UploadPartRequest]],
        * which returns a [[CompletedPart]].
        * In the case it is not, the chunk is added to the buffer waiting to be aggregated with the next one,
        * or in case it was the last chunk [[onComplete()]] will be called and will flush the buffer.
        */
      def onNext(chunk: Array[Byte]): Future[Ack] = {
        buffer = buffer ++ chunk
        if (buffer.length < minChunkSize) Future(Ack.Continue)
        else {
          {
            for {
              uid           <- uploadId
              partNMvar     <- partNMVarEval
              partN         <- partNMvar.take // acquire
              completedPart <- uploadPart(bucket, key, partN, uid, buffer)
              _ <- Task {
                completedParts = completedParts :+ completedPart
                buffer = Array.emptyByteArray
              }
              _   <- partNMvar.put(partN + 1) // release
              ack <- Task.now(Ack.Continue)
            } yield ack
          }.onErrorRecover {
            case NonFatal(ex) => {
              onError(ex)
              Ack.Stop
            }
          }.runToFuture
        }
      }

      /**
        * Checks that the buffer is not empty and performs the last part request [[CompletedPart]] of the buffer in case it was not.
        * Finally it creates a request to indicate that the multipart upload was completed [[CompleteMultipartUploadRequest]],
        * returning a [[CompleteMultipartUploadResponse]] that is passed to the callback.
        */
      def onComplete(): Unit = {
        val response: Task[CompleteMultipartUploadResponse] = for {
          uid       <- uploadId
          partMVar  <- partNMVarEval
          lastPartN <- partMVar.read.timeout(uploadSettings.lastUploadTimeout) // waits for the last upload to finish
          _ <- {
            if (!buffer.isEmpty) { //uploads the last part if buffer is not empty
              for {
                lastPart <- uploadPart(bucket, key, lastPartN + 1, uid, buffer)
              } yield (completedParts = completedParts :+ lastPart)
            } else {
              Task.unit
            }
          }
          completeMultipartUpload <- {
            val request = S3RequestBuilder
              .completeMultipartUploadRequest(bucket, key, uid, completedParts, uploadSettings.requestPayer)
            Task.from(s3Client.completeMultipartUpload(request)) // completes the multipart upload
          }
        } yield completeMultipartUpload
        response.runAsync(callback)
      }

      def onError(ex: Throwable): Unit =
        callback.onError(ex)
    }

    (out, AssignableCancelable.single())
  }

  /**
    * Abstracts the functionality to upload a single part to the S3 object with [[UploadPartRequest]].
    * It should be called either when the received chunk is bigger than minimum size,
    * or later when the last chunk arrives representing the completion of the stream.
    */
  def uploadPart(
    bucket: String,
    key: String,
    partNumber: Int,
    uploadId: String,
    chunk: Array[Byte]): Task[CompletedPart] = {
    for {
      request <- Task {
        S3RequestBuilder.uploadPartRequest(
          bucket = bucket,
          key = key,
          partN = partNumber,
          uploadId = uploadId,
          contentLength = chunk.size.toLong,
          uploadSettings)
      }
      completedPart <- Task
        .from(s3Client.uploadPart(request, AsyncRequestBody.fromBytes(chunk)))
        .map(resp => S3RequestBuilder.completedPart(partNumber, resp))
    } yield completedPart
  }

}
