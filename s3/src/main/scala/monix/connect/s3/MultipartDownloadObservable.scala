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

import monix.connect.s3.S3.{download, listObjects}
import monix.connect.s3.domain.{DefaultDownloadSettings, DownloadSettings}
import monix.eval.Task
import monix.execution.internal.InternalApi
import monix.execution.{Ack, Cancelable}
import monix.reactive.observers.Subscriber
import monix.reactive.Observable
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.GetObjectRequest

@InternalApi
private[s3] class MultipartDownloadObservable(
  bucket: String,
  key: String,
  chunkSize: Long = domain.awsMinChunkSize,
  downloadSettings: DownloadSettings = DefaultDownloadSettings,
  s3AsyncClient: S3AsyncClient)
  extends Observable[Array[Byte]] { self =>

  require(chunkSize > 0, "Chunk size must be a positive number.")

  private[this] val resizedChunk: Long = chunkSize - 1L
  private[this] val firstChunkRange = s"bytes=0-${resizedChunk}"
  private[this] val initialRequest: GetObjectRequest =
    S3RequestBuilder.getObjectRequest(bucket, key, Some(firstChunkRange), downloadSettings)

  def unsafeSubscribeFn(subscriber: Subscriber[Array[Byte]]): Cancelable = {
    val scheduler = subscriber.scheduler
    val f = {
      for {
        s3Object <- {
          listObjects(bucket, prefix = Some(key), maxTotalKeys = Some(1))(s3AsyncClient).headL.onErrorHandleWith { ex =>
            subscriber.onError(ex)
            Task.raiseError(ex)
          }
        }
        chunks <- downloadChunk(subscriber, s3Object.size(), chunkSize, initialRequest, 0)(s3AsyncClient)
      } yield chunks
    }.runToFuture(scheduler)
    f
  }

  private[this] def downloadChunk(
    sub: Subscriber[Array[Byte]],
    totalSize: Long,
    chunkSize: Long,
    getRequest: GetObjectRequest,
    offset: Int)(implicit s3AsyncClient: S3AsyncClient): Task[Unit] = {
    println(s"Download chunk on offset: ${offset}")

    for {
      chunk <- {
        download(getRequest).onErrorHandleWith { ex =>
          sub.onError(ex)
          Task.raiseError(ex)
        }
      }
      ack <- Task.fromFuture(sub.onNext(chunk))
      nextChunk <- {
        ack match {
          case Ack.Continue => {
            val nextOffset = offset + chunk.size
            if (nextOffset < totalSize) {
              val nextRange = s"bytes=${nextOffset}-${nextOffset + chunkSize}"
              val nextRequest = getRequest.toBuilder.range(nextRange).build()
              downloadChunk(sub, totalSize, chunkSize, nextRequest, nextOffset)
            } else {
              sub.onComplete()
              Task.unit
            }
          }
          case Ack.Stop => {
            sub.onComplete()
            Task.unit
          }
        }
      }
    } yield nextChunk
  }

}
