package monix.connect.es

import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class ElasticsearchSinkSuite extends AnyFlatSpecLike with Fixture with Matchers with BeforeAndAfterEach {
  import com.sksamuel.elastic4s.http.ElasticDsl._
  override def beforeEach(): Unit = {
    super.beforeEach()
    elasticClient.execute(deleteIndex("*")).runSyncUnsafe()
  }
  "ElasticsearchSink" should "update a element in batches" in {
    val updateRequests = Gen.listOfN(10, genUpdateRequest).sample.get
    Observable
      .from(updateRequests)
      .bufferTumbling(5)
      .consumeWith(ElasticsearchSink.bulk)
      .runSyncUnsafe()
    val r = updateRequests.map { request =>
      getById(request.indexAndType.index, request.id)
        .map(_.sourceAsString)
        .runSyncUnsafe()
    }
    r should contain theSameElementsAs updateRequests.flatMap(_.documentSource)
  }
}
