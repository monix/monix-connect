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

package monix.connect.hdfs

import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.{Consumer, Observable}
import org.apache.hadoop.fs.{FileSystem, Path}

/*
 * An idiomatic monix service client for operating with HDFS.
 * Built on top of the generic filesystem abstraction from [[org.apache.hadoop.fs.FileSystem]].
 */
object Hdfs {

  /**
    * The subscriber implementation of an HDFS append writer.
    *
    * @param fs        An abstract base class for a fairly generic filesystem.
    * @param path      Names a file or directory in a [[FileSystem]]. Path strings use slash as the directory separator.
    * @param scheduler An implicit [[Scheduler]] instance to be in the scope of the call.
    * @return A [[Long]] that represents the number of bytes that has been written.
    */
  def append(fs: FileSystem, path: Path, lineSeparator: Option[String] = None)(
    implicit
    scheduler: Scheduler): Consumer[Array[Byte], Long] = {
    new HdfsSubscriber(fs, path, appendEnabled = true, lineSeparator = lineSeparator)
  }

  /**
    * The subscriber implementation of an HDFS writer.
    * I expects chunks of bytes to be passed and it synchronously writes them to the specified [[path]].
    * If the chunk is an empty byte array, then no bytes are written, so nothing happens.
    *
    * @param fs          An abstract base class for a fairly generic filesystem. The [[FileSystem]] allows to implement the
    *                    IO operations in the Hadoop DFS (being a multi-machine system) as a single one.
    * @param path        Names a file or directory in a [[FileSystem]]. Path strings use slash as the directory separator.
    * @param overwrite   When a file with this name already exists, then if true, the file will be overwritten.
    *                    And if false an [[java.io.IOException]] will be thrown.
    *                    Files are overwritten by default.
    * @param replication The replication factor.
    * @param bufferSize  The size of the buffer to be used.
    * @param blockSize   The default block size for new files, in bytes. Being 128 MB the default value.
    * @param scheduler   An implicit [[Scheduler]] instance to be in the scope of the call.
    * @return A [[Long]] that represents the number of bytes that has been written.
    */
  def write(
    fs: FileSystem,
    path: Path,
    overwrite: Boolean = true,
    replication: Short = 3,
    bufferSize: Int = 4096,
    blockSize: Int = 134217728,
    lineSeparator: Option[String] = None)(implicit scheduler: Scheduler): Consumer[Array[Byte], Long] = {
    new HdfsSubscriber(fs, path, overwrite, bufferSize, replication, blockSize, appendEnabled = false, lineSeparator)
  }

  /**
    *
    * @param fs        An abstract base class for a fairly generic filesystem.
    *                  Any potentially usage of the Hadoop Distributed File System should be written to use a FileSystem object.
    * @param path      Names a file or directory in a [[FileSystem]]. Path strings use slash as the directory separator.
    * @param chunkSize The maximum length of the emitted arrays of bytes.
    * @param scheduler An implicit [[Scheduler]] instance to be in the scope of the call.
    * @return An [[Observable]] of chunks of bytes with the size specified by [[chunkSize]].
    */
  def read(fs: FileSystem, path: Path, chunkSize: Int = 8192)(
    implicit
    scheduler: Scheduler): Observable[Array[Byte]] = {
    Observable.fromInputStream(Task(fs.open(path)), chunkSize)
  }

}
