package monix.connect.es

import com.sksamuel.elastic4s.Indexes
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class ElasticsearchSuite extends AnyFlatSpecLike with Fixture with Matchers with BeforeAndAfterEach {
  import com.sksamuel.elastic4s.ElasticDsl._
  override def beforeEach(): Unit = {
    super.beforeEach()
    client.execute(deleteIndex("*")).runSyncUnsafe()
  }

  "Elasticsearch" should "execute many update requests" in {
    // given
    val updateRequests = Gen.listOfN(10, genUpdateRequest).sample.get

    // when
    Elasticsearch.bulkRequest(updateRequests).runSyncUnsafe()

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

  it should "execute many delete requests" in {
    // given
    val updateRequests = Gen.listOfN(10, genUpdateRequest).sample.get
    val deleteRequests = updateRequests.take(5).map(r => deleteById(r.index, r.id))

    // when
    Elasticsearch.bulkRequest(updateRequests).runSyncUnsafe()
    Elasticsearch.bulkRequest(deleteRequests).runSyncUnsafe()

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

  it should "execute many index requests" in {
    // given
    val indexRequests = Gen.listOfN(10, genIndexRequest).sample.get

    // when
    Elasticsearch.bulkRequest(indexRequests).runSyncUnsafe()

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

  it should "execute a single update request" in {
    // given
    val request = genUpdateRequest.sample.get

    // when
    Elasticsearch.singleUpdate(request).runSyncUnsafe()

    // then
    val r = getById(request.index.name, request.id)
      .map(_.sourceAsString)
      .runSyncUnsafe()
    r shouldBe request.documentSource.get

  }

  it should "execute a single delete by id request" in {
    // given
    val updateRequests = Gen.listOfN(2, genUpdateRequest).sample.get
    val deleteRequest = deleteById(updateRequests.head.index, updateRequests.head.id)

    // when
    updateRequests.map(Elasticsearch.singleUpdate(_).runSyncUnsafe())
    Elasticsearch.singleDeleteById(deleteRequest).runSyncUnsafe()

    // then
    val r = updateRequests.map { request =>
      getById(request.index.name, request.id)
        .map(_.sourceAsString)
        .runSyncUnsafe()
    }
    r should contain theSameElementsAs List("{}") ++ updateRequests.takeRight(1).flatMap(_.documentSource)
  }

  it should "execute a single delete by query request" in {
    // given
    val updateRequest = genUpdateRequest.sample.get
    val deleteRequest = deleteByQuery(updateRequest.index, idsQuery(updateRequest.id))

    // when
    Elasticsearch.singleUpdate(updateRequest).runSyncUnsafe()
    Elasticsearch.refresh(Seq(updateRequest.index.name)).runSyncUnsafe()
    Elasticsearch.singleDeleteByQuery(deleteRequest).runSyncUnsafe()
    Elasticsearch.refresh(Seq(updateRequest.index.name)).runSyncUnsafe()

    // then
    val r = Elasticsearch
      .searchRequest(search(updateRequest.index).query(matchAllQuery()))
      .map(_.result.hits.total.value)
      .runSyncUnsafe()
    r shouldBe 0
  }

  it should "execute a single search request" in {
    // given
    val updateRequest = genUpdateRequest.sample.get
    val searchRequest = search(updateRequest.index.name).query(matchAllQuery())

    // when
    Elasticsearch.singleUpdate(updateRequest).runSyncUnsafe()
    Elasticsearch.refresh(Seq(updateRequest.index.name)).runSyncUnsafe()
    val r = Elasticsearch.searchRequest(searchRequest).runSyncUnsafe()

    // then
    r.result.hits.hits.head.sourceAsString shouldBe updateRequest.documentSource.get
  }

  it should "execute a create index request" in {
    // given
    val indexName = genIndex.sample.get
    val indexSource = """{"settings":{"number_of_shards":1},"mappings":{"properties":{"a":{"type":"text"}}}}"""
    val createIndexRequest = createIndex(indexName).source(indexSource)

    // when
    Elasticsearch.createIndex(createIndexRequest).runSyncUnsafe()

    // then
    val indexResult = client
      .execute(getIndex(indexName))
      .runSyncUnsafe()
      .result(indexName)
    val mappings = indexResult.mappings.properties("a")
    val settings = indexResult.settings("index.number_of_shards")
    mappings.`type`.get shouldBe "text"
    settings shouldBe "1"
  }

  it should "execute a delete index request" in {
    // given
    val indexName = genIndex.sample.get
    val indexSource = """{"settings":{"number_of_shards":1},"mappings":{"properties":{"a":{"type":"text"}}}}"""
    val createIndexRequest = createIndex(indexName).source(indexSource)
    val deleteIndexRequest = deleteIndex(indexName)

    // when
    Elasticsearch.createIndex(createIndexRequest).runSyncUnsafe()
    Elasticsearch.deleteIndex(deleteIndexRequest).runSyncUnsafe()

    // then
    intercept[NoSuchElementException] {
      client
        .execute(getIndex(indexName))
        .runSyncUnsafe()
        .result
    }
  }

  it should "execute a single count request" in {
    // given
    val index = genIndex.sample.get
    val updateRequests = Gen.listOfN(100, genUpdateRequest(index)).sample.get
    val countRequest = count(Indexes(index)).query(matchAllQuery())

    // when
    Task.parSequence(updateRequests.map(Elasticsearch.singleUpdate)).runSyncUnsafe()
    Elasticsearch.refresh(Seq(index)).runSyncUnsafe()
    val r = Elasticsearch.singleCount(countRequest).runSyncUnsafe()

    // then
    r.result.count shouldBe updateRequests.map(_.id).distinct.length
  }
}
