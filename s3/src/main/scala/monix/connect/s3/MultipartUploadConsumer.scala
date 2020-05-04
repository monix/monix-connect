/*
 * Copyright (c) 2014-2020 by The Monix Connect Project Developers.
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

package monix.connect.s3

import monix.eval.Task
import monix.execution.{Ack, Callback, Scheduler}
import monix.execution.cancelables.AssignableCancelable
import monix.reactive.Consumer
import monix.reactive.observers.Subscriber
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{
  CompleteMultipartUploadResponse,
  CompletedPart,
  CreateMultipartUploadRequest,
  UploadPartRequest
}

/**
  * Implement the functionality to perform an S3 multipart update from received chunks of data.
 */
private[s3] class MultipartUploadConsumer(
  bucket: String,
  key: String,
  minChunkSize: Int = awsMinChunkSize,
  contentType: Option[String] = None)(
  implicit
  s3Client: S3AsyncClient)
  extends Consumer.Sync[Array[Byte], Task[CompleteMultipartUploadResponse]] {

  def createSubscriber(
    callback: Callback[Throwable, Task[CompleteMultipartUploadResponse]],
    s: Scheduler): (Subscriber.Sync[Array[Byte]], AssignableCancelable) = {
    val out = new Subscriber.Sync[Array[Byte]] {
      implicit val scheduler = s
      private var partN = 0
      private var completedParts: List[Task[CompletedPart]] = List.empty[Task[CompletedPart]]
      private val createRequest: CreateMultipartUploadRequest =
        S3RequestBuilder.createMultipartUploadRequest(bucket, key, contentType)
      private var buffer: Array[Byte] = Array.emptyByteArray
      private val uploadId: Task[String] =
        Task
          .from(s3Client.createMultipartUpload(createRequest))
          .map(_.uploadId())
          .memoize

      /**
      * 
       * If the chunk lenght is bigger than the minimum size, perfom the part request,
       * in case it is not, the chunk is added to the buffer waiting the next chunk to be sent together.
       */
      def onNext(chunk: Array[Byte]): Ack = {
        if (chunk.length < minChunkSize) {
          buffer = buffer ++ chunk
        } else {
          partN += 1
          completedParts = {
            for {
              uid           <- uploadId
              completedPart <- completePart(bucket, key, partN, uid, buffer)
            } yield completedPart
          } :: completedParts
          buffer = Array.emptyByteArray
        }
        monix.execution.Ack.Continue
      }

      /**
      * Checks that the buffer is not empty and performs the last part request of the buffer in case it was not.
       * Finally it creates a request to indicate that the multipart upload was completed,
       * and it is passed to the callback.
       */
      def onComplete(): Unit = {
        if (!buffer.isEmpty) {
          completedParts = {
            for {
              uid           <- uploadId
              completedPart <- completePart(bucket, key, partN, uid, buffer)
            } yield completedPart
          } :: completedParts
        }
        val response: Task[CompleteMultipartUploadResponse] = for {
          uid            <- uploadId
          completedParts <- Task.sequence(completedParts)
          completeMultipartUpload <- {
            val request = S3RequestBuilder.completeMultipartUploadRquest(bucket, key, uid, completedParts)
            Task.from(s3Client.completeMultipartUpload(request))
          }
        } yield completeMultipartUpload
        callback.onSuccess(response)
      }

      def onError(ex: Throwable): Unit =
        callback.onError(ex)
    }
    (out, AssignableCancelable.single())
  }

  /**
  * Abstracts the functionality to upload a single part to S3.
   * It is called for those received chunks that were bigger than minimum size,
   * or later when the stream is completed and the last part represents the rest of the buffer.
   */
  def completePart(
    bucket: String,
    key: String,
    partNumber: Int,
    uploadId: String,
    chunk: Array[Byte]): Task[CompletedPart] = {
    val request: UploadPartRequest =
      S3RequestBuilder.uploadPartRequest(bucket, key, partNumber, uploadId, chunk.size.toLong)
    Task
      .from(s3Client.uploadPart(request, AsyncRequestBody.fromBytes(chunk)))
      .map(resp => S3RequestBuilder.completedPart(partNumber, resp))
  }

}
