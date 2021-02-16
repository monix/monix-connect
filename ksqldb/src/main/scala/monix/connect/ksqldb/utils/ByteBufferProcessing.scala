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

package monix.connect.ksqldb.utils

import java.nio.ByteBuffer

import monix.execution.Ack

import monix.execution.Scheduler.Implicits.global
import monix.execution.atomic.AtomicBoolean

import scala.concurrent.Future

/**
  * Processing for data in byte buffer and make separate strings
  * @author Andrey Romanov and @quelgar. Original source: https://gist.github.com/quelgar/3e8aa15d3211f4c791acceeeef6beeef
  */
class ByteBufferProcessing(subscriber: AtomicSubscriber, stopOnNext: AtomicBoolean) {

  def onNextBuffer(buffer: ByteBuffer, elemBuf: ByteBuffer, delimiter: ByteBuffer): Future[Ack] = {

    if (stopOnNext.get() || subscriber.get().isEmpty) {
      Ack.Stop
    } else if (elemBuf.hasRemaining) {

      if (elemBuf.remaining() > buffer.remaining()) {

        elemBuf.limit(elemBuf.position() + buffer.remaining())
        buffer.put(elemBuf)
        elemBuf.limit(elemBuf.capacity())

      } else {
        buffer.put(elemBuf)
      }
      buffer.flip()
      searchDelimiter(elemBuf, buffer, delimiter)

    } else {
      Ack.Continue
    }

  }

  private def searchDelimiter(
    elemBuf: ByteBuffer,
    buffer: ByteBuffer,
    delimiter: ByteBuffer
  ): Future[Ack] = {

    val initLimit: Int = buffer.limit()
    val initPosition: Int = buffer.position()

    if (ByteBufferUtils.findInBuffer(delimiter, buffer)) {

      buffer.limit(buffer.position())
      buffer.position(initPosition)

      val out = new Array[Byte](buffer.remaining())

      buffer.get(out)
      buffer.limit(initLimit)
      buffer.position(buffer.position() + delimiter.remaining())

      subscriber.get().get.onNext(out).flatMap {
        case Ack.Stop => Ack.Stop
        case Ack.Continue => searchDelimiter(elemBuf, buffer, delimiter)
      }

    } else {
      buffer.compact()

      onNextBuffer(buffer, elemBuf, delimiter)
    }

  }

}
