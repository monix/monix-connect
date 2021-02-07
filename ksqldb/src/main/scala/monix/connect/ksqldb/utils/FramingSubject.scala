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

import monix.execution.atomic.{Atomic, AtomicBoolean}
import monix.execution.exceptions
import monix.execution.{Ack, Cancelable}
import monix.reactive.observers.Subscriber
import monix.reactive.subjects.Subject

import scala.concurrent.Future

/**
  * Main subject for process data
  * @param delimiter symbol to split
  * @param maximumLength max symbol length
  * @author Andrey Romanov and @quelgar. Original source: https://gist.github.com/quelgar/3e8aa15d3211f4c791acceeeef6beeef
  */
class FramingSubject(delimiter: ByteBuffer, maximumLength: Int) extends Subject[Array[Byte], Array[Byte]] {

  private[this] val subscriber: AtomicSubscriber = Atomic(Option.empty[Subscriber[Array[Byte]]])
  private[this] val stopOnNext: AtomicBoolean = Atomic(false)

  private var buffer: ByteBuffer = _

  override def size: Int = if (this.subscriber.get().nonEmpty) 1 else 0

  override def onNext(elem: Array[Byte]): Future[Ack] = {

    val bufferProcessing = new ByteBufferProcessing(subscriber, stopOnNext)

    bufferProcessing.onNextBuffer(buffer, ByteBuffer.wrap(elem), delimiter)
  }

  override def onError(ex: Throwable): Unit = subscriber.get().foreach(_.onError(ex))

  override def onComplete(): Unit = subscriber.get().foreach(_.onComplete())

  override def unsafeSubscribeFn(subscriberFn: Subscriber[Array[Byte]]): Cancelable = {

    if (!this.subscriber.compareAndSet(None, Some(subscriberFn))) {

      subscriberFn.onError(exceptions.APIContractViolationException(this.getClass.getName))
      Cancelable.empty

    } else {
      buffer = ByteBuffer.allocate(maximumLength)
      Cancelable(() => stopOnNext.set(true))
    }

  }
}
