package scalona.monix.connect.common

import akka.stream.Materializer
import akka.stream.scaladsl.{ Keep, Sink, Source }
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.cancelables.SingleAssignCancelable
import monix.reactive.{ Consumer, Observable, Observer }
import org.reactivestreams.{ Publisher, Subscriber }

import scala.concurrent.Future

object AkkaInterOp {

  object Implicits {

    implicit class ExtendedAkkaSink[-In, Out <: Future[_]](sink: Sink[In, Out]) {
      def asConsumer[M](implicit materializer: Materializer, scheduler: Scheduler): Consumer[In, Task[M]] = {
        val (sub: Subscriber[In], mat: Future[M]) = Source.asSubscriber[In].toMat(sink)(Keep.both).run()
        val observer = Observer.fromReactiveSubscriber[In](sub, SingleAssignCancelable())
        val consumer = Consumer.fromObserver[In](implicit scheduler => observer).map(_ => Task.fromFuture[M](mat))
        consumer
      }
    }

    implicit class ExtendedAkkaSource[In, Out](source: Source[In, Out]) {
      def asObservable(implicit materializer: Materializer, scheduler: Scheduler): Observable[In] = {
        val (_: Out, pub: Publisher[In]) = source.toMat(Sink.asPublisher(true))(Keep.both).run()
        Observable.fromReactivePublisher[In](pub)
      }
    }

  }

}
