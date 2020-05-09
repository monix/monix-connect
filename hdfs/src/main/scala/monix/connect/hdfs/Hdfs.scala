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

import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.{Consumer, Observable}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.CompressionCodec

object Hdfs {

  /**
   * The subscriber implementation of an HDFS append writer.
   * I expects chunks of bytes to be passed and it synchronously writes them to the specified [[path]].
   * If the chunk is an empty byte array, then no bytes are written, so nothing happens.
   *
   * @param fs An abstract base class for a fairly generic filesystem.
   *           Any potentially usage of the Hadoop Distributed File System should be written to use a FileSystem object.
   *           The Hadoop DFS is a multi-machine system that appears as a single disk.
   *           It's useful because of its fault tolerance and potentially very large capacity.
   * @param path Names a file or directory in a [[FileSystem]]. Path strings use slash as the directory separator.
   * @param scheduler An implicit [[Scheduler]] instance to be in the scope of the call.
   * @return A [[Long]] that represents the number of bytes that has been written.
   */
  def append(fs: FileSystem, path: Path)(implicit scheduler: Scheduler): Consumer[Array[Byte], Long] = {
    new HdfsSubscriber(fs, path, appendEnabled = true)
  }

  /**
   * The subscriber implementation of an HDFS writer.
   * I expects chunks of bytes to be passed and it synchronously writes them to the specified [[path]].
   * If the chunk is an empty byte array, then no bytes are written, so nothing happens.
   *
   * @param fs An abstract base class for a fairly generic filesystem.
   *           Any potentially usage of the Hadoop Distributed File System should be written to use a FileSystem object.
   *           The Hadoop DFS is a multi-machine system that appears as a single disk.
   *           It's useful because of its fault tolerance and potentially very large capacity.
   * @param path Names a file or directory in a [[FileSystem]]. Path strings use slash as the directory separator.
   * @param scheduler An implicit [[Scheduler]] instance to be in the scope of the call.
   * @return A [[Long]] that represents the number of bytes that has been written.
   */
  def write(fs: FileSystem, path: Path)(implicit scheduler: Scheduler): Consumer[Array[Byte], Long] = {
    new HdfsSubscriber(fs, path)
  }

  /**
   *
   * @param fs An abstract base class for a fairly generic filesystem.
   *           Any potentially usage of the Hadoop Distributed File System should be written to use a FileSystem object.
   * @param path Names a file or directory in a [[FileSystem]]. Path strings use slash as the directory separator.
   * @param chunkSize The maximum length of the emitted arrays of bytes.
   * @param scheduler An implicit [[Scheduler]] instance to be in the scope of the call.
   * @return An [[Observable]] of chunks of bytes with the size specified by [[chunkSize]].
   */
  def read(fs: FileSystem, path: Path, chunkSize: Int = 8192)(
    implicit
    scheduler: Scheduler): Observable[Array[Byte]] = {
    Observable.fromInputStream(Task(fs.open(path)), chunkSize)
  }

  /**
   *
   * @param fs An abstract base class for a fairly generic filesystem.
   *           Any potentially usage of the Hadoop Distributed File System should be written to use a FileSystem object.
   * @param path Names a file or directory in a [[FileSystem]]. Path strings use slash as the directory separator.
   * @param chunkSize The maximum length of the emitted arrays of bytes.
   * @param codec An instance of a [[CompressionCodec]], that will encapsulates the logic of a streaming compression/decompression.
   * @param scheduler An implicit [[Scheduler]] instance to be in the scope of the call.
   * @return An [[Observable]] of chunks of bytes with the size specified by [[chunkSize]].
   */
  def readCompressed(fs: FileSystem, path: Path, chunkSize: Int = 8192, codec: CompressionCodec)(
    implicit
    scheduler: Scheduler): Observable[Array[Byte]] = {
    Observable.fromInputStream(Task(codec.createInputStream(fs.open(path))), chunkSize)
  }

}
