package monix.connect.es

import com.sksamuel.elastic4s.bulk.BulkCompatibleRequest
import com.sksamuel.elastic4s.http.{ElasticClient, RequestFailure, RequestSuccess}
import monix.execution.cancelables.AssignableCancelable
import monix.execution.internal.InternalApi
import monix.execution.{Ack, Callback, Scheduler}
import monix.reactive.Consumer
import monix.reactive.observers.Subscriber

import scala.concurrent.Future


/**
  *  A pre-built [[Consumer]] implementation that expects incoming [[BulkCompatibleRequest]]
  *
  * @param client an implicit instance of a [[ElasticClient]]
  */
@InternalApi private[es] class ElasticsearchSink(implicit client: ElasticClient)
  extends Consumer[Seq[BulkCompatibleRequest], Unit] {
  override def createSubscriber(
    cb: Callback[Throwable, Unit],
    s: Scheduler
  ): (Subscriber[Seq[BulkCompatibleRequest]], AssignableCancelable) = {

    val subscriber = new Subscriber[Seq[BulkCompatibleRequest]] {
      override implicit def scheduler: Scheduler = s

      override def onNext(elem: Seq[BulkCompatibleRequest]): Future[Ack] = {
        Elasticsearch
          .bulkRequest(elem)
          .map {
            case RequestSuccess(_, _, _, _) =>
              Ack.Continue
            case RequestFailure(status, _, _, error) =>
              onError(new RuntimeException(s"$status: $error"))
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

object ElasticsearchSink {
  def bulk(implicit client: ElasticClient): ElasticsearchSink = {
    new ElasticsearchSink()
  }
}
