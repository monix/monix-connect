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
import monix.execution.{Ack, Callback, Scheduler}
import monix.execution.cancelables.AssignableCancelable
import monix.reactive.Consumer
import monix.reactive.observers.Subscriber
import org.apache.hadoop.fs.{FileSystem, Path}

class HdfsSubscriber(fs: FileSystem, path: Path) extends Consumer.Sync[Array[Byte], Task[Int]] {

  def createSubscriber(
    callback: Callback[Throwable, Task[Int]],
    s: Scheduler): (Subscriber.Sync[Array[Byte]], AssignableCancelable) = {
    val sub = new Subscriber.Sync[Array[Byte]] {
      val out = fs.create(path)
      var off = 0

      override implicit def scheduler: Scheduler = s

      override def onComplete() = {
        out.close()
        callback.onSuccess(Task(off))
      }

      override def onError(ex: Throwable): Unit = {
        callback.onError(ex)
      }

      override def onNext(elem: Array[Byte]): Ack = {
        val len = elem.length
        out.write(elem, off, len)
        off += len
        Ack.Continue
      }
    }

    (sub, AssignableCancelable.dummy)
  }

}
