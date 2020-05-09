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

package monix.connect.hdfs

import monix.execution.cancelables.AssignableCancelable
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
 * @param fs
 * @param path
 * @param overwrite   When a file with this name already exists, then if true, the file will be overwritten.
 *                    And if false an [[java.io.IOException]] will be thrown.
 *                    Files are overwritten by default.
 * @param bufferSize  The size of the buffer to be used.
 * @param replication The replication factor.
 * @param blockSize   The default block size for new files, in bytes. Being 128 MB the default value.
 */
private[hdfs] class HdfsSubscriber(fs: FileSystem,
                                    path: Path,
                                    overwrite: Boolean = true,
                                    bufferSize: Int = 4096,
                                    replication: Short = 3,
                                    blockSize: Int = 134217728,
                                   appendEnabled: Boolean = false) extends Consumer.Sync[Array[Byte], Long] {

  def createSubscriber(
                        callback: Callback[Throwable, Long],
                        s: Scheduler): (Subscriber.Sync[Array[Byte]], AssignableCancelable) = {
    val sub = new Subscriber.Sync[Array[Byte]] {
      val out: FSDataOutputStream = createOrAppendFS(fs, path, appendEnabled, overwrite, bufferSize, replication, blockSize)
      var off: Long = 0

      override implicit def scheduler: Scheduler = s

      override def onComplete() = {
        out.close()
        callback.onSuccess(off)
      }

      override def onError(ex: Throwable): Unit = {
        out.close()
        callback.onError(ex)
      }

      override def onNext(chunk: Array[Byte]): Ack = {
        val len: Int = chunk.size
        try {
          out.write(chunk)
        } catch { case e if NonFatal(e) => callback.onError(e) }
        off += len
        Ack.Continue
      }
    }

    (sub, AssignableCancelable.single)
  }

  /**
   * A builder for creating an instance of [[FSDataOutputStream]] that
   * @return
   */
  protected def createOrAppendFS(fs: FileSystem,
                               path: Path,
                               appendEnabled: Boolean,
                               overwrite: Boolean,
                               bufferSize: Int,
                               replication: Short,
                               blockSize: Int): FSDataOutputStream = {
    if (appendEnabled) {
      fs.append(path, bufferSize)
    }
    else fs.create(path, overwrite, bufferSize, replication, blockSize)
  }
}


