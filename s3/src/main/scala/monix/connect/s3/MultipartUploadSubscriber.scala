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

package monix.connect.s3

import monix.connect.s3.domain.UploadSettings
import monix.eval.Task
import monix.execution.cancelables.AssignableCancelable
import monix.execution.{Ack, Callback, Scheduler}
import monix.reactive.observers.Subscriber
import monix.reactive.Consumer
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model._

import scala.concurrent.Future
import scala.util.control.NonFatal

/**
  * An Amazon S3 multipart uploads is a write operation made
  * progressively on individual chunks of an object (parts),
  * that finishes with a message to S3 that multipart upload is completed,
  * in which the concatenation of each of the individual chunks will end up
  * stored into the same object.
  */
private[s3] class MultipartUploadSubscriber(
  bucket: String,
  key: String,
  minChunkSize: Int,
  uploadSettings: UploadSettings)(
  implicit
  s3Client: S3AsyncClient)
  extends Consumer[Array[Byte], CompleteMultipartUploadResponse] {

  def createSubscriber(
    callback: Callback[Throwable, CompleteMultipartUploadResponse],
    s: Scheduler): (Subscriber[Array[Byte]], AssignableCancelable) = {
    val out = new Subscriber[Array[Byte]] {

      implicit val scheduler = s
      private val createRequest: CreateMultipartUploadRequest =
        S3RequestBuilder.createMultipartUploadRequest(bucket, key, uploadSettings)
      private val uploadId: Task[String] =
        Task
          .from(s3Client.createMultipartUpload(createRequest))
          .map(_.uploadId())
          .memoize
      private var buffer: Array[Byte] = Array.emptyByteArray
      private var completedParts: List[CompletedPart] = List.empty[CompletedPart]
      private var partN = 1

      /**
        *
        * If the chunk lenght is bigger than the minimum size, perfom the part request [[UploadPartRequest]],
        * which returns a [[CompletedPart]].
        * In the case it is not, the chunk is added to the buffer waiting to be aggregated with the next one.
        */
      def onNext(chunk: Array[Byte]): Future[Ack] = {
        if (chunk.length < minChunkSize) {
          buffer = buffer ++ chunk
          Future(Ack.Continue)
        } else {
          {
            for {
              uid           <- uploadId
              completedPart <- uploadPart(bucket, key, partN, uid, buffer)
              _ <- Task {
                completedParts = completedPart :: completedParts
                buffer = Array.emptyByteArray
                partN += 1
              }
              ack <- Task(Ack.Continue)
            } yield ack
          }.onErrorRecoverWith {
            case NonFatal(ex) => {
              onError(ex)
              Task.now(Ack.Stop)
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
          uid <- uploadId
          _ <- Task.defer {
            if (!buffer.isEmpty) { //perform the last update if buffer is not empty
              for {
                lastPart <- uploadPart(bucket, key, partN, uid, buffer)
                _ <- Task {
                  completedParts = lastPart :: completedParts
                }
              } yield ()
            } else {
              Task.unit
            }
          }
          completeMultipartUpload <- {
            val request = S3RequestBuilder
              .completeMultipartUploadRquest(bucket, key, uid, completedParts, uploadSettings.requestPayer)
            Task.from(s3Client.completeMultipartUpload(request))
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
    * Abstracts the functionality to upload a single part to S3 with [[UploadPartRequest]].
    * It should only be called when the received for chunks bigger than minimum size,
    * or later when the last chunk arrives representing the completion of the stream.
    */
  def uploadPart(bucket: String, key: String, partNumber: Int, uploadId: String, chunk: Array[Byte])(
    implicit
    scheduler: Scheduler): Task[CompletedPart] = {
    for {
      request <- Task {
        S3RequestBuilder.uploadPartRequest(
          bucket = bucket,
          key = key,
          partN = partNumber,
          uploadId = uploadId,
          contentLenght = chunk.size.toLong,
          uploadSettings)
      }
      completedPart <- Task
        .from(s3Client.uploadPart(request, AsyncRequestBody.fromBytes(chunk)))
        .map(resp => S3RequestBuilder.completedPart(partNumber, resp))
    } yield completedPart
  }

}

object MultipartUploadSubscriber {

  //Multipart upload minimum part size
  val awsMinChunkSize: Int = 5 * 1024 * 1024 //5242880

}
