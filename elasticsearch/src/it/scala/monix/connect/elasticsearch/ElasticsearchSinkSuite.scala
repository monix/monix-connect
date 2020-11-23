package monix.connect.elasticsearch

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.util.Try

class ElasticsearchSinkSuite extends AnyFlatSpecLike with Fixture with Matchers with BeforeAndAfterEach {
  import com.sksamuel.elastic4s.ElasticDsl._

  "ElasticsearchSink" should "execute update requests in batches" in {
    // given
    val updateRequests = Gen.listOfN(10, genUpdateRequest).sample.get

    // when
    esResource.use { es =>
      Observable
        .from(updateRequests)
        .bufferTumbling(5)
        .consumeWith(es.createBulkRequestsSink())
    }.runSyncUnsafe()

    // then
    val r =
      Task
        .parSequence(updateRequests.map { request =>
          getById(request.index.name, request.id)
            .map(_.sourceAsString)
        })
        .runSyncUnsafe()
    r should contain theSameElementsAs updateRequests.flatMap(_.documentSource)
  }

  it should "execute delete requests in batches" in {
    // given
    val updateRequests = Gen.listOfN(10, genUpdateRequest).sample.get
    val deleteRequests = updateRequests.take(5).map(r => deleteById(r.index, r.id))

    // when
    esResource.use { es =>
      Observable
        .from(updateRequests)
        .bufferTumbling(5)
        .consumeWith(es.createBulkRequestsSink())
    }.runSyncUnsafe()
    esResource.use { es =>
      Observable
        .from(deleteRequests)
        .bufferTumbling(5)
        .consumeWith(es.createBulkRequestsSink())
    }.runSyncUnsafe()
    // then
    val r =
      Task
        .parSequence(updateRequests.map { request =>
          getById(request.index.name, request.id)
            .map(_.sourceAsString)
        })
        .runSyncUnsafe()
    r should contain theSameElementsAs List.fill(5)("{}") ++ updateRequests.takeRight(5).flatMap(_.documentSource)
  }

  it should "execute index requests batches" in {
    // given
    val indexRequests = Gen.listOfN(10, genIndexRequest).sample.get

    // when
    esResource.use { es =>
      Observable
        .from(indexRequests)
        .bufferTumbling(5)
        .consumeWith(es.createBulkRequestsSink())
    }.runSyncUnsafe()

    // then
    val r =
      Task
        .parSequence(indexRequests.map { request =>
          getById(request.index.name, request.id.get)
            .map(_.sourceAsString)
        })
        .runSyncUnsafe()
    r should contain theSameElementsAs indexRequests.flatMap(_.source)
  }

  it should "fails when the es index not exists" in {
    // given
    val requests = Seq(updateById("test_index", "test_id"))

    // when
    val ob =
      esResource.use { es =>
        Observable
          .from(requests)
          .bufferTumbling(5)
          .consumeWith(es.createBulkRequestsSink())
      }

    // then
    val r = Try(ob.runSyncUnsafe())
    r.isFailure shouldBe true
  }
}
