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

import scala.jdk.FutureConverters._

private[s3] class MultipartUploadConsumer(bucket: String, key: String, minChunkSize: Int = 5 * 1024 * 1024, contentType: Option[String] = None)(
  implicit
  s3Client: S3AsyncClient)
  extends Consumer.Sync[Array[Byte], Task[CompleteMultipartUploadResponse]] {

  def createSubscriber(
    callback: Callback[Throwable, Task[CompleteMultipartUploadResponse]],
    s: Scheduler): (Subscriber.Sync[Array[Byte]], AssignableCancelable) = {
    val out = new Subscriber.Sync[Array[Byte]] {
      implicit val scheduler = s
      private[this] val partN = 0
      private[this] var completedParts: List[Task[CompletedPart]] = List()
      private[this] val multiPartUploadrequest: CreateMultipartUploadRequest =
        S3RequestBuilder.multipartUploadRequest(bucket, key, contentType)
      private var stashed: Array[Byte] = Array.emptyByteArray
      private[this] val uploadId: Task[String] =
        Task
          .fromFuture(
            s3Client
              .createMultipartUpload(multiPartUploadrequest)
              .asScala
              .map(_.uploadId()))
          .memoize
      println("Uuid: " + uploadId.runSyncUnsafe())

      def onNext(chunk: Array[Byte]): Ack = {
        if (chunk.length < 2000) {
          stashed = stashed ++ chunk
        } else {
          completedParts = {
            for {
              uid           <- uploadId
              completedPart <- completePart(bucket, key, partN, uid, stashed)
            } yield completedPart
          } :: completedParts
          stashed = Array.emptyByteArray
        }
        monix.execution.Ack.Continue
      }

      def onComplete(): Unit = {
        if (!stashed.isEmpty) {
          completedParts = {
            for {
              uid           <- uploadId
              completedPart <- completePart(bucket, key, partN, uid, stashed)
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

    (out, AssignableCancelable.dummy)
  }

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
