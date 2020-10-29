package monix.connect.es

import com.sksamuel.elastic4s.requests.searches.{SearchHit, SearchRequest, SearchResponse}
import com.sksamuel.elastic4s.{ElasticClient, RequestFailure, RequestSuccess, Response}
import monix.connect.es.ElasticsearchSource.SearchSettings
import monix.eval.Task
import monix.execution.internal.InternalApi
import monix.execution.{Ack, Cancelable, Scheduler}
import monix.reactive.Observable
import monix.reactive.observers.Subscriber

import scala.collection.mutable

/**
  *  A pre-built [[monix.reactive.Observable]] implementation that
  *  publishes documents using an elasticsearch scroll cursor.
  *
  * @param settings settings for how documents are queried
  * @param client an implicit instance of a [[ElasticClient]]
  */
@InternalApi private[es] class ElasticsearchSource(settings: SearchSettings)(implicit client: ElasticClient)
  extends Observable[SearchHit] {

  import com.sksamuel.elastic4s.ElasticDsl._

  require(settings.search.keepAlive.isDefined, "The SearchRequest must have a scroll defined (a keep alive time)")
  private var scrollId: String = _
  // Parse the keep alive setting out of the original query.
  private val keepAlive = settings.search.keepAlive.getOrElse("1m")

  override def unsafeSubscribeFn(subscriber: Subscriber[SearchHit]): Cancelable = {
    runLoop(mutable.Queue.empty, subscriber).runToFuture(subscriber.scheduler)
  }

  private def runLoop(buffer: mutable.Queue[SearchHit], sub: Subscriber[SearchHit]): Task[Unit] = {
    fastLoop(buffer, sub).flatMap {
      case Ack.Continue =>
        runLoop(buffer, sub)
      case Ack.Stop =>
        sub.onComplete()
        Task.unit
    }
  }

  private def fastLoop(buffer: mutable.Queue[SearchHit], sub: Subscriber[SearchHit]): Task[Ack] = {
    implicit val s: Scheduler = sub.scheduler
    maybeFetch(buffer, sub).flatMap { _ =>
      if (buffer.nonEmpty)
        Task.deferFuture(sub.onNext(buffer.dequeue))
      else
        Task.now(Ack.Stop)
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
            scrollId = id
            if (result.hits.hits.length == 0) {
              sub.onComplete()
            } else {
              buffer ++= result.hits.hits
            }
        }
    }
  }

  private def maybeFetch(buffer: mutable.Queue[SearchHit], sub: Subscriber[SearchHit]): Task[Unit] = {
    if (buffer.isEmpty)
      fetch(buffer, sub)
    else
      Task.unit
  }

  // if no fetch is in progress then fire one
  private def fetch(buffer: mutable.Queue[SearchHit], sub: Subscriber[SearchHit]): Task[Unit] = {
    Option(scrollId) match {
      case None => client.execute(settings.search).map(populateHandler(_, buffer, sub))
      case Some(id) => client.execute(searchScroll(id).keepAlive(keepAlive)).map(populateHandler(_, buffer, sub))
    }
  }
}

object ElasticsearchSource {
  case class SearchSettings(search: SearchRequest)

  def search(settings: SearchSettings)(implicit client: ElasticClient): ElasticsearchSource =
    new ElasticsearchSource(settings)
}
