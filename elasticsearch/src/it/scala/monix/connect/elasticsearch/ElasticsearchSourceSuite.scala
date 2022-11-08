package monix.connect.elasticsearch

import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.Scheduler.Implicits.global
import monix.testing.scalatest.MonixTaskTest
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.{AnyFlatSpecLike, AsyncFlatSpec}
import org.scalatest.matchers.should.Matchers

import scala.util.Try

class ElasticsearchSourceSuite extends AsyncFlatSpec with MonixTaskTest with Fixture with Matchers with BeforeAndAfterEach {
  import com.sksamuel.elastic4s.ElasticDsl._

  override implicit val scheduler: Scheduler = Scheduler.io("elasticsearch-sink-suite")

  "ElasticsearchSource" should "emit searched results to downstream" in {
    val index = genIndex.sample.get
    val updateRequests = Gen.listOfN(1000, genUpdateRequest(index)).sample.get
    val searchRequest = search(index).query(matchAllQuery()).keepAlive("1m")

    esResource.use { es =>
      Task
        .sequence(
          Seq(
            es.bulkExecuteRequest(updateRequests),
            es.refresh(index)
          )
        ) *>
      es.scroll(searchRequest)
        .map(_.id)
        .toListL
    }.asserting(_ should contain theSameElementsAs updateRequests.map(_.id).distinct)
  }

  it should "emit searched results when the param `keepAlive` is empty" in {
    val index = genIndex.sample.get
    val updateRequests = Gen.listOfN(1000, genUpdateRequest(index)).sample.get
    val searchRequest = search(index).query(matchAllQuery())

    esResource.use { es =>
      Task.sequence(
        Seq(
          es.bulkExecuteRequest(updateRequests),
          es.refresh(index)
        )
      ) *>
      es.scroll(searchRequest).map(_.id).toListL
    }.asserting(_ should contain theSameElementsAs updateRequests.map(_.id).distinct)
  }

  it should "fails when the es index not exists" in {
    val index = genIndex.sample.get
    val searchRequest = search(index).query(matchAllQuery()).keepAlive("1m")

    esResource.use(_.scroll(searchRequest).map(_.id).toListL)
      .attempt.asserting(_.isLeft shouldBe true)
  }

  it should "returns an empty list when result is empty" in {
    val index = genIndex.sample.get
    val searchRequest = search(index).query(matchAllQuery()).keepAlive("1m")

    esResource.use{ es =>
      es.createIndex(createIndex(index)) *>
        es.scroll(searchRequest).map(_.id).toListL
    }.asserting(_.isEmpty shouldBe true)
  }

}
