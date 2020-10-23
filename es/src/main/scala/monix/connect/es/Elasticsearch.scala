package monix.connect.es

import com.sksamuel.elastic4s.bulk.BulkCompatibleRequest
import com.sksamuel.elastic4s.http._
import com.sksamuel.elastic4s.http.bulk.BulkResponse
import monix.eval.Task

/**
  * Object for managing the Elasticsearch
  */
object Elasticsearch {
  import com.sksamuel.elastic4s.http.ElasticDsl._

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
}
