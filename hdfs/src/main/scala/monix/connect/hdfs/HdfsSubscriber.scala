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

package monix.connect.hdfs

import monix.execution.cancelables.AssignableCancelable
import monix.execution.internal.Platform
import monix.execution.{Ack, Callback, Scheduler}
import monix.reactive.Consumer
import monix.reactive.observers.Subscriber
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

import scala.util.control.NonFatal

/**
  * A subscriber implementation for writing to HDFS.
  *
  * @see https://hadoop.apache.org/docs/r2.8.2/api/org/apache/hadoop/fs/FileSystem.html
  * @see https://hadoop.apache.org/docs/r0.23.11/hadoop-project-dist/hadoop-common/core-default.xml
  * @param fs          An abstract base class for a fairly generic filesystem.
  * @param path        Names a file or directory in a [[FileSystem]]. Path strings use slash as the directory separator.
  * @param overwrite   When a file with this name already exists, then if true, the file will be overwritten.
  *                    And if false an [[java.io.IOException]] will be thrown.
  *                    Files are overwritten by default.
  * @param bufferSize  The size of the buffer to be used.
  * @param replication The replication factor.
  * @param blockSize   The default block size for new files, in bytes. Being 128 MB the default value.
  */
private[hdfs] class HdfsSubscriber(
  fs: FileSystem,
  path: Path,
  overwrite: Boolean = true,
  bufferSize: Int = 4096,
  replication: Short = 3,
  blockSize: Int = 134217728,
  appendEnabled: Boolean = false)
  extends Consumer.Sync[Array[Byte], Long] {

  def createSubscriber(
    callback: Callback[Throwable, Long],
    s: Scheduler): (Subscriber.Sync[Array[Byte]], AssignableCancelable) = {
    val sub = new Subscriber.Sync[Array[Byte]] {

      override implicit def scheduler: Scheduler = s
      private[this] var isDone = false
      private[this] val out: FSDataOutputStream =
        createOrAppendFS(fs, path, appendEnabled, overwrite, bufferSize, replication, blockSize)
      private[this] var offset: Long = 0

      def onNext(chunk: Array[Byte]): Ack = {
        val len: Int = chunk.size
        try {
          out.write(chunk)
        } catch {
          case NonFatal(e) =>
            callback.onError(e)
        }
        offset += len
        Ack.Continue
      }

      def onComplete(): Unit = {
        if !isDone then {
          isDone = true
          try {
            out.close()
          } catch {
            case NonFatal(ex) => {
              onError(ex)
            }
          }
          callback.onSuccess(offset)
        }
      }

      def onError(ex: Throwable): Unit = {
        if !isDone then {
          isDone = true
          try {
            out.close()
            callback.onError(ex)
          } catch {
            case NonFatal(ex2) => {
              callback.onError(Platform.composeErrors(ex, ex2))
            }
          }
        }
      }

    }

    (sub, AssignableCancelable.single())
  }

  /** A builder for creating an instance of [[FSDataOutputStream]]. */
  protected def createOrAppendFS(
    fs: FileSystem,
    path: Path,
    appendEnabled: Boolean,
    overwrite: Boolean,
    bufferSize: Int,
    replication: Short,
    blockSize: Int): FSDataOutputStream = {
    if appendEnabled then {
      fs.append(path, bufferSize)
    } else fs.create(path, overwrite, bufferSize, replication, blockSize)
  }
}
