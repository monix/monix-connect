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

package monix.connect.akka.stream

import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.cancelables.SingleAssignCancelable
import monix.reactive.{Consumer, Observable, Observer}
import org.reactivestreams.{Publisher, Subscriber}

import scala.concurrent.Future

object Converters {

  implicit class ExtendedAkkaSink[-In, +R <: Future[_]](sink: Sink[In, R]) {
    def asConsumer[Out](implicit materializer: Materializer, scheduler: Scheduler): Consumer[In, Task[Out]] = {
      val (sub: Subscriber[In], mat: Future[Out]) = Source.asSubscriber[In].toMat(sink)(Keep.both).run()
      val observer = Observer.fromReactiveSubscriber[In](sub, SingleAssignCancelable())
      val consumer = Consumer.fromObserver[In](implicit scheduler => observer).map(_ => Task.fromFuture[Out](mat))
      consumer
    }
  }

  implicit class ExtendedAkkaFlow[-In, +Out, +Mat](flow: Flow[In, Out, Mat])
    extends ExtendedAkkaSink[In, Future[Out]](flow.toMat(Sink.last)(Keep.right))

  implicit class ExtendedAkkaSource[+In, +Mat](source: Source[In, Mat]) {
    def asObservable(implicit materializer: Materializer, scheduler: Scheduler): Observable[In] = {
      val pub: Publisher[In] = source.toMat(Sink.asPublisher(fanout = false))(Keep.right).run()
      Observable.fromReactivePublisher[In](pub)
    }

    def consumeWith[Out](
      consumer: Consumer[In, Out])(implicit materializer: Materializer, scheduler: Scheduler): Task[Out] = {
      asObservable(materializer, scheduler).consumeWith(consumer)
    }
  }
}
