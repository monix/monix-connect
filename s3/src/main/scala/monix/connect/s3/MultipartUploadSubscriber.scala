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
  CompleteMultipartUploadRequest,
  CompleteMultipartUploadResponse,
  CompletedPart,
  CreateMultipartUploadRequest,
  UploadPartRequest
}

/**
  * A multipart upload is an upload to Amazon S3 that is creating by uploading individual pieces of an object (parts),
  * then telling Amazon S3 to complete the multipart upload and concatenate all the individual pieces together into a single object.
  * Implement the functionality to perform an S3 multipart update from received chunks of data.
  */
private[s3] class MultipartUploadSubscriber(
  bucket: String,
  key: String,
  minChunkSize: Int = MultipartUploadSubscriber.awsMinChunkSize,
  acl: Option[String],
  contentType: Option[String] = None,
  grantFullControl: Option[String],
  grantRead: Option[String],
  grantReadACP: Option[String],
  grantWriteACP: Option[String],
  serverSideEncryption: Option[String] = None,
  sseCustomerAlgorithm: Option[String] = None,
  sseCustomerKey: Option[String] = None,
  sseCustomerKeyMD5: Option[String] = None,
  ssekmsEncryptionContext: Option[String],
  ssekmsKeyId: Option[String],
  requestPayer: Option[String])(
  implicit
  s3Client: S3AsyncClient)
  extends Consumer.Sync[Array[Byte], Task[CompleteMultipartUploadResponse]] {

  def createSubscriber(
    callback: Callback[Throwable, Task[CompleteMultipartUploadResponse]],
    s: Scheduler): (Subscriber.Sync[Array[Byte]], AssignableCancelable) = {
    val out = new Subscriber.Sync[Array[Byte]] {
      implicit val scheduler = s
      private var partN = 1
      private var completedParts: List[Task[CompletedPart]] = List.empty[Task[CompletedPart]]
      private val createRequest: CreateMultipartUploadRequest =
        S3RequestBuilder.createMultipartUploadRequest(
          bucket = bucket,
          key = key,
          contentType = contentType,
          acl = acl,
          grantFullControl = grantFullControl,
          grantRead = grantRead,
          grantReadACP = grantReadACP,
          grantWriteACP = grantWriteACP,
          requestPayer = requestPayer,
          serverSideEncryption = serverSideEncryption,
          sseCustomerAlgorithm = sseCustomerAlgorithm,
          sseCustomerKey = sseCustomerKey,
          sseCustomerKeyMD5 = sseCustomerKeyMD5,
          ssekmsEncryptionContext = ssekmsEncryptionContext,
          ssekmsKeyId = ssekmsKeyId)
      private var buffer: Array[Byte] = Array.emptyByteArray
      private val uploadId: Task[String] =
        Task
          .from(s3Client.createMultipartUpload(createRequest))
          .map(_.uploadId())
          .memoize

      /**
        *
        * If the chunk lenght is bigger than the minimum size, perfom the part request [[UploadPartRequest]],
        * which returns a [[CompletedPart]].
        * In the case it is not, the chunk is added to the buffer waiting to be aggregated with the next one.
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
        * Checks that the buffer is not empty and performs the last part request [[CompletedPart]] of the buffer in case it was not.
        * Finally it creates a request to indicate that the multipart upload was completed [[CompleteMultipartUploadRequest]],
        * returning a [[CompleteMultipartUploadResponse]] that is passed to the callback.
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
            val request = S3RequestBuilder.completeMultipartUploadRquest(bucket, key, uid, completedParts, requestPayer)
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
    * Abstracts the functionality to upload a single part to S3 with [[UploadPartRequest]].
    * It should only be called when the received for chunks bigger than minimum size,
    * or later when the last chunk arrives representing the completion of the stream.
    */
  def completePart(
    bucket: String,
    key: String,
    partNumber: Int,
    uploadId: String,
    chunk: Array[Byte]): Task[CompletedPart] = {
    val request: UploadPartRequest =
      S3RequestBuilder.uploadPartRequest(
        bucket = bucket,
        key = key,
        partN = partNumber,
        uploadId = uploadId,
        contentLenght = chunk.size.toLong,
        sseCustomerAlgorithm = sseCustomerAlgorithm,
        sseCustomerKey = sseCustomerKey,
        sseCustomerKeyMD5 = sseCustomerKeyMD5
      )
    Task
      .from(s3Client.uploadPart(request, AsyncRequestBody.fromBytes(chunk)))
      .map(resp => S3RequestBuilder.completedPart(partNumber, resp))
  }

}

object MultipartUploadSubscriber {

  //Multipart upload minimum part size
  val awsMinChunkSize: Int = 5 * 1024 * 1024 //5242880

}
