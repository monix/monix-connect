package cloriko.monix.connect.akka

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.cancelables.SingleAssignCancelable
import monix.reactive.{Consumer, Observable, Observer}
import org.reactivestreams.{Publisher, Subscriber}

import scala.concurrent.Future

object AkkaStreams {

  object Implicits {

    implicit class ExtendedAkkaSink[-In, Out <: Future[_]](sink: Sink[In, Out]) {
      def asConsumer[M](implicit materializer: Materializer, scheduler: Scheduler): Consumer[In, Task[M]] = {
        val (sub: Subscriber[In], mat: Future[M]) = Source.asSubscriber[In].toMat(sink)(Keep.both).run()
        val observer = Observer.fromReactiveSubscriber[In](sub, SingleAssignCancelable())
        val consumer = Consumer.fromObserver[In](implicit scheduler => observer).map(_ => Task.fromFuture[M](mat))
        consumer
      }
    }

    implicit class ExtendedAkkaFlow[-In, Out, Mat](flow: Flow[In, Out, Mat])
      extends ExtendedAkkaSink[In, Future[Out]](flow.toMat(Sink.last)(Keep.right))

    implicit class ExtendedAkkaSource[In, Out](source: Source[In, Out]) {
      def asObservable(implicit materializer: Materializer, scheduler: Scheduler): Observable[In] = {
        val pub: Publisher[In] = source.toMat(Sink.asPublisher(fanout = false))(Keep.right).run()
        Observable.fromReactivePublisher[In](pub)
      }
    }

  }

  lazy val sharedActorSystem = ActorSystem("Monix-Connect")

}
