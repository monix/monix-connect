package monix.connect.es

import com.sksamuel.elastic4s.requests.bulk.{BulkCompatibleRequest, BulkResponse}
import com.sksamuel.elastic4s.requests.count.{CountRequest, CountResponse}
import com.sksamuel.elastic4s.requests.delete._
import com.sksamuel.elastic4s.requests.indexes._
import com.sksamuel.elastic4s.requests.indexes.admin.{DeleteIndexResponse, RefreshIndexResponse}
import com.sksamuel.elastic4s.requests.searches.{SearchRequest, SearchResponse}
import com.sksamuel.elastic4s.requests.update.{UpdateRequest, UpdateResponse}
import com.sksamuel.elastic4s.{ElasticClient, Response}
import monix.eval.Task

/**
  * Object for managing the Elasticsearch
  */
object Elasticsearch {

  import com.sksamuel.elastic4s.ElasticDsl._

  /**
    * Execute bulk requests
    *
    * @param requests some [[BulkCompatibleRequest]]
    * @param client an implicit instance of a [[ElasticClient]]
    * @return a [[Task]] with [[Response]] with [[BulkResponse]]
    */
  def bulkRequest(requests: Seq[BulkCompatibleRequest])(
    implicit client: ElasticClient
  ): Task[Response[BulkResponse]] = {
    client.execute(bulk(requests))
  }

  /**
    * Execute a single update request
    *
    * @param request a update request
    * @param client an implicit instance of a [[ElasticClient]]
    * @return a [[Task]] with [[Response]] with [[UpdateResponse]]
    */
  def singleUpdate(request: UpdateRequest)(
    implicit client: ElasticClient
  ): Task[Response[UpdateResponse]] = {
    client.execute(request)
  }

  /**
    * Execute a single search request
    *
    * @param request a search request
    * @param client an implicit instance of a [[ElasticClient]]
    * @return a [[Task]] with [[Response]] with [[SearchResponse]]
    */
  def searchRequest(request: SearchRequest)(
    implicit client: ElasticClient
  ): Task[Response[SearchResponse]] = {
    client.execute(request)
  }

  /**
    * Execute a single delete by id request
    *
    * @param request a delete by id request
    * @param client an implicit instance of a [[ElasticClient]]
    * @return a [[Task]] with [[Response]] with [[DeleteResponse]]
    */
  def singleDeleteById(request: DeleteByIdRequest)(
    implicit client: ElasticClient
  ): Task[Response[DeleteResponse]] = {
    client.execute(request)
  }

  /**
    * Execute a single delete by query request
    *
    * @param request a delete by query request
    * @param client an implicit instance of a [[ElasticClient]]
    * @return a [[Task]] with [[Response]] with [[DeleteByQueryResponse]]
    */
  def singleDeleteByQuery(request: DeleteByQueryRequest)(
    implicit client: ElasticClient
  ): Task[Response[DeleteByQueryResponse]] = {
    client.execute(request)
  }

  /**
    * Execute a single create index request
    *
    * @param request a create index request
    * @param client an implicit instance of a [[ElasticClient]]
    * @return a [[Task]] with [[Response]] with [[CreateIndexRequest]]
    */
  def createIndex(request: CreateIndexRequest)(
    implicit client: ElasticClient
  ): Task[Response[CreateIndexResponse]] = {
    client.execute(request)
  }

  /**
    * Execute a single delete index request
    *
    * @param request a delete index request
    * @param client an implicit instance of a [[ElasticClient]]
    * @return a [[Task]] with [[Response]] with [[DeleteIndexResponse]]
    */
  def deleteIndex(request: DeleteIndexRequest)(
    implicit client: ElasticClient
  ): Task[Response[DeleteIndexResponse]] = {
    client.execute(request)
  }

  /**
    * Execute a single count request
    *
    * @param request a count request
    * @param client an implicit instance of a [[ElasticClient]]
    * @return a [[Task]] with [[Response]] with [[CountResponse]]
    */
  def singleCount(request: CountRequest)(
    implicit client: ElasticClient
  ): Task[Response[CountResponse]] = {
    client.execute(request)
  }

  /**
    * Refresh indexes
    *
    * @param indexes names of indexes
    * @param client an implicit instance of a [[ElasticClient]]
    * @return a [[Task]] with [[Response]] with [[RefreshIndexResponse]]
    */
  def refresh(indexes: Iterable[String])(
    implicit client: ElasticClient
  ): Task[Response[RefreshIndexResponse]] = {
    client.execute(refreshIndex(indexes))
  }

  def refresh(first: String, rest: String*)(
    implicit client: ElasticClient
  ): Task[Response[RefreshIndexResponse]] =
    refresh(first +: rest)
}
