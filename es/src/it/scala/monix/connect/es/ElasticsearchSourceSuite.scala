package monix.connect.es

import monix.execution.Scheduler.Implicits.global
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._
import scala.util.Try

class ElasticsearchSourceSuite extends AnyFlatSpecLike with Fixture with Matchers with BeforeAndAfterEach {
  import com.sksamuel.elastic4s.ElasticDsl._
  override def beforeEach(): Unit = {
    super.beforeEach()
    client.execute(deleteIndex("*")).runSyncUnsafe()
  }

  "ElasticsearchSource" should "emit searched results to downstream" in {
    // given
    val index = genIndex.sample.get
    val updateRequests = Gen.listOfN(1000, genUpdateRequest(index)).sample.get
    val searchRequest = search(index).query(matchAllQuery()).keepAlive("1m")
    val source = ElasticsearchSource.search(searchRequest)

    // when
    Elasticsearch.bulkRequest(updateRequests).runSyncUnsafe()
    Elasticsearch.refresh(index).runSyncUnsafe()

    // then
    val r = source
      .bufferTimedAndCounted(100.millis, 100)
      .map(_.map(_.id))
      .toListL
      .runSyncUnsafe()
      .flatten
    r should contain theSameElementsAs updateRequests.map(_.id).distinct
  }

  it should "emit searched results when the param `keepAlive` is empty" in {
    // given
    val index = genIndex.sample.get
    val updateRequests = Gen.listOfN(1000, genUpdateRequest(index)).sample.get
    val searchRequest = search(index).query(matchAllQuery())
    val source = ElasticsearchSource.search(searchRequest)

    // when
    Elasticsearch.bulkRequest(updateRequests).runSyncUnsafe()
    Elasticsearch.refresh(index).runSyncUnsafe()

    // then
    val r = source.map(_.id).toListL.runSyncUnsafe()
    r should contain theSameElementsAs updateRequests.map(_.id).distinct
  }

  it should "fails when the es index not exists" in {
    // given
    val index = genIndex.sample.get
    val searchRequest = search(index).query(matchAllQuery()).keepAlive("1m")
    val source = ElasticsearchSource.search(searchRequest)

    // when
    val result = Try(source.map(_.id).toListL.runSyncUnsafe())

    // then
    result.isFailure shouldBe true
  }

  it should "returns an empty list when result is empty" in {
    // given
    val index = genIndex.sample.get
    Elasticsearch.createIndex(createIndex(index)).runSyncUnsafe()
    val searchRequest = search(index).query(matchAllQuery()).keepAlive("1m")
    val source = ElasticsearchSource.search(searchRequest)

    // when
    val result = source.map(_.id).toListL.runSyncUnsafe()

    // then
    result.isEmpty shouldBe true
  }

}
