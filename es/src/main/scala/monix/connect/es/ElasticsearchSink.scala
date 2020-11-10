package monix.connect.es

import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.requests.bulk.BulkCompatibleRequest
import monix.execution.cancelables.AssignableCancelable
import monix.execution.internal.InternalApi
import monix.execution.{Ack, Callback, Scheduler}
import monix.reactive.Consumer
import monix.reactive.observers.Subscriber

import scala.concurrent.Future

/**
  *  A pre-built [[Consumer]] implementation that expects incoming [[BulkCompatibleRequest]]
  *
  * @param es an instance of a [[Elasticsearch]]
  */
@InternalApi private[es] class ElasticsearchSink(es: Elasticsearch) extends Consumer[Seq[BulkCompatibleRequest], Unit] {
  override def createSubscriber(
    cb: Callback[Throwable, Unit],
    s: Scheduler
  ): (Subscriber[Seq[BulkCompatibleRequest]], AssignableCancelable) = {

    val subscriber = new Subscriber[Seq[BulkCompatibleRequest]] {
      override implicit def scheduler: Scheduler = s

      override def onNext(elem: Seq[BulkCompatibleRequest]): Future[Ack] = {
        es.bulkExecuteRequest(elem)
          .map {
            case RequestSuccess(_, _, _, _) =>
              Ack.Continue
            case RequestFailure(_, _, _, error) =>
              onError(error.asException)
              Ack.Stop
          }
          .runToFuture
      }

      override def onError(ex: Throwable): Unit = {
        cb.onError(ex)
      }

      override def onComplete(): Unit = {
        cb.onSuccess()
      }
    }
    (subscriber, AssignableCancelable.single())
  }
}
