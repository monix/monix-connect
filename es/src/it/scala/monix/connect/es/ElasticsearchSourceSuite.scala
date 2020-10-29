package monix.connect.es

import monix.execution.Scheduler.Implicits.global
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.util.Try

class ElasticsearchSourceSuite extends AnyFlatSpecLike with Fixture with Matchers with BeforeAndAfterEach {
  import com.sksamuel.elastic4s.ElasticDsl._
  override def beforeEach(): Unit = {
    super.beforeEach()
    client.execute(deleteIndex("*")).runSyncUnsafe()
  }

  "ElasticsearchSource" should "fails when there is no keep alive time" in {
    // given
    val index = "test_index"
    val searchRequest = search(index).query(matchAllQuery())
    val searchSettings = ElasticsearchSource.SearchSettings(searchRequest)

    // when
    val source = Try(ElasticsearchSource.search(searchSettings))

    // then
    source.isFailure shouldBe true
  }

  it should "search and return results" in {
    // given
    val index = "test_index"
    val updateRequests = Gen.listOfN(1000, genUpdateRequest(index)).sample.get
    val searchRequest = search(index).query(matchAllQuery()).keepAlive("1m")
    val searchSettings = ElasticsearchSource.SearchSettings(searchRequest)
    val source = ElasticsearchSource.search(searchSettings)

    // when
    Elasticsearch.bulkRequest(updateRequests).runSyncUnsafe()
    Elasticsearch.refresh(index).runSyncUnsafe()

    // then
    val r = source.map(_.id).toListL.runSyncUnsafe()
    r should contain theSameElementsAs updateRequests.map(_.id).distinct
  }

  it should "fails when es error occurs" in {
    // given
    val index = "test_index"
    val searchRequest = search(index).query(matchAllQuery()).keepAlive("1m")
    val searchSettings = ElasticsearchSource.SearchSettings(searchRequest)
    val source = ElasticsearchSource.search(searchSettings)

    // when
    val result = Try(source.map(_.id).toListL.runSyncUnsafe())

    // then
    result.isFailure shouldBe true
  }

  it should "returns an empty list when result is empty" in {
    // given
    val index = "test_index"
    Elasticsearch.createIndex(createIndex(index)).runSyncUnsafe()
    val searchRequest = search(index).query(matchAllQuery()).keepAlive("1m")
    val searchSettings = ElasticsearchSource.SearchSettings(searchRequest)
    val source = ElasticsearchSource.search(searchSettings)

    // when
    val result = source.map(_.id).toListL.runSyncUnsafe()

    // then
    result.isEmpty shouldBe true
  }

}
