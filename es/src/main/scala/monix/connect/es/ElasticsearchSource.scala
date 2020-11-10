package monix.connect.es

import com.sksamuel.elastic4s.requests.searches.{SearchHit, SearchRequest, SearchResponse}
import com.sksamuel.elastic4s.{ElasticClient, RequestFailure, RequestSuccess, Response}
import monix.eval.Task
import monix.execution.internal.InternalApi
import monix.execution.{Ack, Cancelable}
import monix.reactive.Observable
import monix.reactive.observers.Subscriber

import scala.collection.mutable

/**
  *  A pre-built [[monix.reactive.Observable]] implementation that
  *  publishes documents using an elasticsearch scroll cursor.
  *
  * @param request [[SearchRequest]]
  * @param client an implicit instance of a [[ElasticClient]]
  */
@InternalApi private[es] class ElasticsearchSource(request: SearchRequest)(implicit client: ElasticClient)
  extends Observable[SearchHit] {

  import com.sksamuel.elastic4s.ElasticDsl._

  private var scrollId: Option[String] = None

  private val keepAlive = request.keepAlive.getOrElse("1m")

  override def unsafeSubscribeFn(subscriber: Subscriber[SearchHit]): Cancelable = {
    fastLoop(mutable.Queue.empty, subscriber).runToFuture(subscriber.scheduler)
  }

  private def fastLoop(buffer: mutable.Queue[SearchHit], sub: Subscriber[SearchHit]): Task[Unit] = {
    fetch(buffer, sub).flatMap { _ =>
      if (buffer.nonEmpty)
        Task.deferFuture(sub.onNext(buffer.dequeue))
      else
        Task.now(Ack.Stop)
    }.flatMap {
      case Ack.Continue =>
        fastLoop(buffer, sub)
      case Ack.Stop =>
        sub.onComplete()
        Task.unit
    }
  }

  private def populateHandler(
    response: Response[SearchResponse],
    buffer: mutable.Queue[SearchHit],
    sub: Subscriber[SearchHit]
  ): Unit = {
    response match {
      case RequestFailure(_, _, _, error) =>
        sub.onError(error.asException)
      case RequestSuccess(_, _, _, result) =>
        result.scrollId match {
          case None =>
            sub.onError(new RuntimeException("Search response did not include a scroll id"))
          case Some(id) =>
            scrollId = Some(id)
            if (result.hits.hits.length == 0 && buffer.isEmpty) {
              sub.onComplete()
            } else {
              buffer ++= result.hits.hits
            }
        }
    }
  }

  private def fetch(buffer: mutable.Queue[SearchHit], sub: Subscriber[SearchHit]): Task[Unit] = {
    if (buffer.isEmpty) {
      scrollId match {
        case Some(id) => client.execute(searchScroll(id).keepAlive(keepAlive)).map(populateHandler(_, buffer, sub))
        case None => client.execute(request.keepAlive(keepAlive)).map(populateHandler(_, buffer, sub))
      }
    } else {
      Task.unit
    }
  }
}

object ElasticsearchSource {
  def search(request: SearchRequest)(implicit client: ElasticClient): ElasticsearchSource =
    new ElasticsearchSource(request)
}
