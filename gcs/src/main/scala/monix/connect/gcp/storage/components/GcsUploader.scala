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

package monix.connect.gcp.storage.components

import java.nio.ByteBuffer

import com.google.cloud.WriteChannel
import com.google.cloud.storage.{BlobInfo, Storage}
import com.google.cloud.storage.Storage.BlobWriteOption
import monix.connect.gcp.storage.GcsStorage
import monix.execution.cancelables.AssignableCancelable
import monix.execution.internal.Platform
import monix.execution.{Ack, Callback, Scheduler}
import monix.reactive.Consumer
import monix.reactive.observers.Subscriber

import scala.util.control.NonFatal

/**
  * Monix [[Consumer]] implementation for uploading an
  * unbounded number of byte arrays to a single Blob.
  */
private[storage] final class GcsUploader(
  storage: Storage,
  blobInfo: BlobInfo,
  chunkSize: Int,
  options: BlobWriteOption*)
  extends Consumer[Array[Byte], Unit] {

  override def createSubscriber(
    cb: Callback[Throwable, Unit],
    s: Scheduler): (Subscriber[Array[Byte]], AssignableCancelable) = {
    val sub = new Subscriber.Sync[Array[Byte]] {
      self =>
      override implicit def scheduler: Scheduler = s

      val writer: WriteChannel = storage.writer(blobInfo, options: _*)
      writer.setChunkSize(chunkSize)
      private[this] var isDone = false

      override def onNext(chunk: Array[Byte]): Ack = {
        if (chunk.isEmpty) {
          onComplete()
          Ack.Stop
        } else {
          try {
            writer.write(ByteBuffer.wrap(chunk))
            monix.execution.Ack.Continue
          } catch {
            case NonFatal(ex) => {
              onError(ex)
              Ack.Stop
            }
          }
        }
      }

      override def onComplete(): Unit = {
        if (!isDone) {
          isDone = true
          try {
            writer.close()
            cb.onSuccess(())
          } catch {
            case NonFatal(ex) => {
              onError(ex)
            }
          }
        }
      }

      override def onError(ex: Throwable): Unit = {
        if (!isDone) {
          isDone = true
          try {
            writer.close()
            cb.onError(ex)
          } catch {
            case NonFatal(ex2) => {
              cb.onError(Platform.composeErrors(ex, ex2))
            }
          }
        }
      }
    }

    (sub, AssignableCancelable.dummy)
  }
}

/** Companion object of [[GcsUploader]]. */
private[storage] object GcsUploader {
  def apply(
    storage: GcsStorage,
    blobInfo: BlobInfo,
    chunkSize: Int = 4096,
    options: List[BlobWriteOption] = List.empty): GcsUploader =
    new GcsUploader(storage.underlying, blobInfo, chunkSize, options: _*)
}
