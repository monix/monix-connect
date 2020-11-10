/*
 * Copyright (c) 2020-2020 by The Monix Connect Project Developers.
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

package monix.connect.es

import cats.effect.Resource
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.requests.bulk.{BulkCompatibleRequest, BulkResponse}
import com.sksamuel.elastic4s.requests.count.{CountRequest, CountResponse}
import com.sksamuel.elastic4s.requests.delete._
import com.sksamuel.elastic4s.requests.get.{GetRequest, GetResponse}
import com.sksamuel.elastic4s.requests.indexes._
import com.sksamuel.elastic4s.requests.indexes.admin.{DeleteIndexResponse, RefreshIndexResponse}
import com.sksamuel.elastic4s.requests.searches.{SearchHit, SearchRequest, SearchResponse}
import com.sksamuel.elastic4s.requests.update.{UpdateRequest, UpdateResponse}
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties, HttpClient, Response}
import monix.eval.Task
import monix.execution.annotations.UnsafeBecauseImpure
import monix.reactive.{Consumer, Observable}

/**
  * Singleton object provides builders for [[Elasticsearch]].
  *
  * ===Example===
  *{{{
  * import com.sksamuel.elastic4s.ElasticDsl._
  * import com.sksamuel.elastic4s._
  * import monix.eval.Task
  *
  * val esResource = Elasticsearch.create("http://localhost:9200")
  *
  * val indexName = "test_index"
  * val indexSource = """{"settings":{"number_of_shards":1},"mappings":{"properties":{"a":{"type":"text"} } } }"""
  * val createIndexRequest = createIndex(indexName).source(indexSource)
  * val id = "test_id"
  * val doc = """{"a":"test"}"""
  * val updateRequest = updateById(indexName, id).docAsUpsert(doc)
  *
  * esResource.use { es =>
  *   Task.sequence(
  *     Seq(
  *       es.createIndex(createIndexRequest),
  *       es.singleUpdate(updateRequest),
  *       es.refresh(Seq(indexName))
  *     )
  *   )
  * }
  * }}}
  */
object Elasticsearch {
  def create(httpClientFactory: => HttpClient): Resource[Task, Elasticsearch] = {
    Resource.make {
      Task.eval {
        val client = ElasticClient(client = httpClientFactory)
        createUnsafe(client)
      }
    } { _.close() }
  }

  /**
    * Creates [[Elasticsearch]] from an URI
    *
    * @param uri an URI for creating es client. format: http(s)://host:port,host:port(/prefix)?querystring
    * @return a [[Elasticsearch]] object
    */
  def create(uri: String): Resource[Task, Elasticsearch] = create(JavaClient(ElasticProperties(uri)))

  @UnsafeBecauseImpure
  def createUnsafe(esClient: ElasticClient): Elasticsearch = {
    new Elasticsearch {
      override val client: ElasticClient = esClient
    }
  }

}

private[es] trait Elasticsearch { self =>
  import com.sksamuel.elastic4s.ElasticDsl._

  private[es] val client: ElasticClient

  /**
    * Execute bulk requests
    *
    * @param requests some [[BulkCompatibleRequest]]
    * @return a [[Task]] with [[Response]] with [[BulkResponse]]
    */
  def bulkExecuteRequest(requests: Seq[BulkCompatibleRequest]): Task[Response[BulkResponse]] = {
    client.execute(bulk(requests))
  }

  /**
    * Get a document by id
    *
    * @param request a [[GetRequest]]
    * @return a [[Task]] with [[Response]] with [[GetResponse]]
    */
  def getById(request: GetRequest): Task[Response[GetResponse]] = {
    client.execute(request)
  }
  /**
    * Get index info
    *
    * @param request a [[GetIndexRequest]]
    * @return a [[Task]] with [[Response]] with [[GetIndexResponse]]
    */
  def getIndex(request: GetIndexRequest): Task[Response[Map[String, GetIndexResponse]]] = {
    client.execute(request)
  }

  /**
    * Execute a single update request
    *
    * @param request a update request
    * @return a [[Task]] with [[Response]] with [[UpdateResponse]]
    */
  def singleUpdate(request: UpdateRequest): Task[Response[UpdateResponse]] = {
    client.execute(request)
  }

  /**
    * Execute a single search request
    *
    * @param request a search request
    * @return a [[Task]] with [[Response]] with [[SearchResponse]]
    */
  def search(request: SearchRequest): Task[Response[SearchResponse]] = {
    client.execute(request)
  }

  /**
    * Execute a single delete by id request
    *
    * @param request a delete by id request
    * @return a [[Task]] with [[Response]] with [[DeleteResponse]]
    */
  def singleDeleteById(request: DeleteByIdRequest): Task[Response[DeleteResponse]] = {
    client.execute(request)
  }

  /**
    * Execute a single delete by query request
    *
    * @param request a delete by query request
    * @return a [[Task]] with [[Response]] with [[DeleteByQueryResponse]]
    */
  def singleDeleteByQuery(request: DeleteByQueryRequest): Task[Response[DeleteByQueryResponse]] = {
    client.execute(request)
  }

  /**
    * Execute a single create index request
    *
    * @param request a create index request
    * @return a [[Task]] with [[Response]] with [[CreateIndexRequest]]
    */
  def createIndex(request: CreateIndexRequest): Task[Response[CreateIndexResponse]] = {
    client.execute(request)
  }

  /**
    * Execute a single delete index request
    *
    * @param request a delete index request
    * @return a [[Task]] with [[Response]] with [[DeleteIndexResponse]]
    */
  def deleteIndex(request: DeleteIndexRequest): Task[Response[DeleteIndexResponse]] = {
    client.execute(request)
  }

  /**
    * Execute a single count request
    *
    * @param request a count request
    * @return a [[Task]] with [[Response]] with [[CountResponse]]
    */
  def singleCount(request: CountRequest): Task[Response[CountResponse]] = {
    client.execute(request)
  }

  /**
    * Refresh indexes
    *
    * @param indexes names of indexes
    * @return a [[Task]] with [[Response]] with [[RefreshIndexResponse]]
    */
  def refresh(indexes: Iterable[String]): Task[Response[RefreshIndexResponse]] = {
    client.execute(refreshIndex(indexes))
  }

  def refresh(first: String, rest: String*): Task[Response[RefreshIndexResponse]] =
    refresh(first +: rest)

  /**
    * Retrieve large sets of results from a single scrolling search request.
    * @param request a [[SearchRequest]] object
    * @return an [[Observable]] that emits the [[SearchHit]]
    */
  def scroll(request: SearchRequest): Observable[SearchHit] = {
    new ElasticsearchSource(request)(client)
  }

  /**
    * Bulk execute es requests
    * @return an [[Consumer]] that receives a list of [[BulkCompatibleRequest]]
    */
  def createBulkRequestsSink(es: Elasticsearch = this): Consumer[Seq[BulkCompatibleRequest], Unit] = {
    new ElasticsearchSink(es)
  }

  /**
    * Closes the underlying [[ElasticClient]].
    */
  def close(): Task[Unit] = Task.evalOnce(client.close())
}
