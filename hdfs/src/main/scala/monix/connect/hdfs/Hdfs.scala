/*
 * Copyright (c) 2014-2020 by The Monix Project Developers.
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

  def write(fs: FileSystem, path: Path)(implicit scheduler: Scheduler): Consumer[Array[Byte], Task[Int]] = {
    new HdfsSubscriber(fs, path)
  }

  def read(fs: FileSystem, path: Path, chunkSize: Int = 8192)(
    implicit
    scheduler: Scheduler): Observable[Array[Byte]] = {
    Observable.fromInputStream(Task(fs.open(path)), chunkSize)
  }

  def readCompressed(fs: FileSystem, path: Path, chunkSize: Int = 8192, codec: CompressionCodec)(
    implicit
    scheduler: Scheduler): Observable[Array[Byte]] = {
    Observable.fromInputStream(Task(codec.createInputStream(fs.open(path))), chunkSize)
  }

}
