package monix.connect.elasticsearch

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.util.Try

class ElasticsearchSourceSuite extends AnyFlatSpecLike with Fixture with Matchers with BeforeAndAfterEach {
  import com.sksamuel.elastic4s.ElasticDsl._

  "ElasticsearchSource" should "emit searched results to downstream" in {
    // given
    val index = genIndex.sample.get
    val updateRequests = Gen.listOfN(1000, genUpdateRequest(index)).sample.get
    val searchRequest = search(index).query(matchAllQuery()).keepAlive("1m")

    // when
    esResource.use { es =>
      Task
        .sequence(
          Seq(
            es.bulkExecuteRequest(updateRequests),
            es.refresh(index)
          )
        )
    }.runSyncUnsafe()
    // then
    val r = esResource.use { es =>
      es.scroll(searchRequest)
        .map(_.id)
        .toListL
    }.runSyncUnsafe()
    r should contain theSameElementsAs updateRequests.map(_.id).distinct
  }

  it should "emit searched results when the param `keepAlive` is empty" in {
    // given
    val index = genIndex.sample.get
    val updateRequests = Gen.listOfN(1000, genUpdateRequest(index)).sample.get
    val searchRequest = search(index).query(matchAllQuery())

    // when
    esResource.use { es =>
      Task.sequence(
        Seq(
          es.bulkExecuteRequest(updateRequests),
          es.refresh(index)
        )
      )
    }.runSyncUnsafe()

    // then
    val r = esResource.use(_.scroll(searchRequest).map(_.id).toListL).runSyncUnsafe()
    r should contain theSameElementsAs updateRequests.map(_.id).distinct
  }

  it should "fails when the es index not exists" in {
    // given
    val index = genIndex.sample.get
    val searchRequest = search(index).query(matchAllQuery()).keepAlive("1m")

    // when
    val r = Try(esResource.use(_.scroll(searchRequest).map(_.id).toListL).runSyncUnsafe())

    // then
    r.isFailure shouldBe true
  }

  it should "returns an empty list when result is empty" in {
    // given
    val index = genIndex.sample.get
    esResource.use(_.createIndex(createIndex(index))).runSyncUnsafe()
    val searchRequest = search(index).query(matchAllQuery()).keepAlive("1m")
    // when
    val result = esResource.use(_.scroll(searchRequest).map(_.id).toListL).runSyncUnsafe()

    // then
    result.isEmpty shouldBe true
  }

}
