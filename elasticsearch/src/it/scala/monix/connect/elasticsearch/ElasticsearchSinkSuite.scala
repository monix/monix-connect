package monix.connect.elasticsearch

import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import monix.testing.scalatest.MonixTaskSpec
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

class ElasticsearchSinkSuite extends AsyncFlatSpec with MonixTaskSpec with Fixture with Matchers with BeforeAndAfterEach {
  import com.sksamuel.elastic4s.ElasticDsl._

  override implicit val scheduler: Scheduler = Scheduler.io("elasticsearch-sink-suite")

  "ElasticsearchSink" should "execute update requests in batches" in {
    val updateRequests = Gen.listOfN(5, genUpdateRequest).sample.get

    esResource.use { es =>
      Observable
        .from(updateRequests)
        .bufferTumbling(5)
        .consumeWith(es.bulkRequestSink()) *>
        Task
          .parTraverse(updateRequests){ request =>
            getById(request.index.name, request.id)
              .map(_.sourceAsString)
          }
    }.asserting(_ should contain allElementsOf updateRequests.flatMap(_.documentSource))
  }

  it should "execute delete requests in batches" in {
    val updateRequests = Gen.listOfN(10, genUpdateRequest).sample.get
    val deleteRequests = updateRequests.take(5).map(r => deleteById(r.index, r.id))

    esResource.use { es =>
      Observable
        .from(updateRequests)
        .bufferTumbling(5)
        .consumeWith(es.bulkRequestSink()) *>
        Observable
          .from(deleteRequests)
          .bufferTumbling(5)
          .consumeWith(es.bulkRequestSink()) *>
        Task
          .parSequence(updateRequests.map { request =>
            getById(request.index.name, request.id)
              .map(_.sourceAsString)
          })
    }.asserting(_ should contain allElementsOf List.fill(5)("{}") ++ updateRequests.takeRight(5).flatMap(_.documentSource))
  }

  it should "execute index requests batches" in {
    val indexRequests = Gen.listOfN(10, genIndexRequest).sample.get

    esResource.use { es =>
      Observable
        .from(indexRequests)
        .bufferTumbling(5)
        .consumeWith(es.bulkRequestSink()) *>
        Task
          .parTraverse(indexRequests){ request =>
            getById(request.index.name, request.id.get)
              .map(_.sourceAsString)
          }
    }.asserting{ _ should contain allElementsOf indexRequests.flatMap(_.source) }
  }

  it should "fails when the es index not exists" in {
    val requests = Seq(updateById("test_index", "test_id"))

   esResource.use { es =>
     Observable
       .from(requests)
       .bufferTumbling(5)
       .consumeWith(es.bulkRequestSink())
   }.attempt.asserting(_.isLeft shouldBe true)
  }
}
