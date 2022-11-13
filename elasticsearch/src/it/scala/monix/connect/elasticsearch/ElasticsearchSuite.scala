package monix.connect.elasticsearch

import com.sksamuel.elastic4s.Indexes
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.exceptions.DummyException
import monix.testing.scalatest.MonixTaskTest
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

class ElasticsearchSuite extends AsyncFlatSpec with MonixTaskTest with Fixture with Matchers with BeforeAndAfterEach {
  import com.sksamuel.elastic4s.ElasticDsl._

  override implicit val scheduler: Scheduler = Scheduler.io("elasticsearch-suite")

  "Elasticsearch" should "execute many update requests" in {
    val updateRequests = Gen.listOfN(10, genUpdateRequest).sample.get

    esResource.use{ es =>
      es.bulkExecuteRequest(updateRequests) *>
        Task
          .parSequence(updateRequests.map { request =>
            getById(request.index.name, request.id)
              .map(_.sourceAsString)
          })
    }.asserting(_ should contain theSameElementsAs updateRequests.flatMap(_.documentSource))
  }

  it should "execute many delete requests" in {
    val updateRequests = Gen.listOfN(10, genUpdateRequest).sample.get
    val deleteRequests = updateRequests.take(5).map(r => deleteById(r.index, r.id))
    val expectedDocuments =  List.fill(5)("{}") ++ updateRequests.takeRight(5).flatMap(_.documentSource)
    esResource.use { es =>
      Task.sequence(
        Seq(
          es.bulkExecuteRequest(updateRequests),
          es.refresh(updateRequests.map(_.index.name)),
          es.bulkExecuteRequest(deleteRequests)
        )
      ) *>
        Task
          .parSequence(updateRequests.map { request =>
            getById(request.index.name, request.id)
              .map(_.sourceAsString)
          })
    }.asserting(_ should contain theSameElementsAs expectedDocuments)
  }

  it should "execute many index requests" in {
    val indexRequests = Gen.listOfN(10, genIndexRequest).sample.get

    esResource.use{
      _.bulkExecuteRequest(indexRequests)
    } *>
      Task
        .parSequence(indexRequests.map { request =>
          getById(request.index.name, request.id.get)
            .map(_.sourceAsString)
        }).asserting{
        _ should contain theSameElementsAs indexRequests.flatMap(_.source)
      }
  }

  it should "execute a single update request" in {
    val request = genUpdateRequest.sample.get

    esResource.use{ _.singleUpdate(request) *>
      getById(request.index.name, request.id)
        .map(_.sourceAsString)
    }.asserting(_ shouldBe request.documentSource.get)
  }

  it should "execute a single delete by id request" in {
    val updateRequests = Gen.listOfN(2, genUpdateRequest).sample.get
    val deleteRequest = deleteById(updateRequests.head.index, updateRequests.head.id)
    val expectedDocs = List("{}") ++ updateRequests.takeRight(1).flatMap(_.documentSource)

    esResource.use { es =>
      Task
        .parSequence(updateRequests.map(es.singleUpdate))
        .flatMap(_ => es.singleDeleteById(deleteRequest)) *>
        Task.traverse(updateRequests) { request =>
          getById(request.index.name, request.id)
            .map(_.sourceAsString)
        }
    }.asserting(_ should contain theSameElementsAs expectedDocs)
  }

  it should "execute a single delete by query request" in {
    val updateRequest = genUpdateRequest.sample.get
    val deleteRequest = deleteByQuery(updateRequest.index, idsQuery(updateRequest.id))

    esResource.use { es =>
      Task.sequence(
        Seq(
          es.singleUpdate(updateRequest),
          es.refresh(Seq(updateRequest.index.name)),
          es.singleDeleteByQuery(deleteRequest),
          es.refresh(Seq(updateRequest.index.name))
        )
      ) *>
       es.search(search(updateRequest.index).query(matchAllQuery()))
          .map(_.result.hits.total.value)
    } asserting(_ shouldBe 0)
  }

  it should "execute a single search request" in {
    val updateRequest = genUpdateRequest.sample.get
    val searchRequest = search(updateRequest.index.name).query(matchAllQuery())

    esResource.use { es =>
      Task
        .sequence(
          Seq(
            es.singleUpdate(updateRequest),
            es.refresh(Seq(updateRequest.index.name))
          )
        )
        .flatMap(_ => es.search(searchRequest))
    }.asserting(_.result.hits.hits.head.sourceAsString shouldBe updateRequest.documentSource.get)
  }

  it should "execute a create index request" in {
    val indexName = genIndex.sample.get
    val indexSource = """{"settings":{"number_of_shards":1},"mappings":{"properties":{"a":{"type":"text"}}}}"""
    val createIndexRequest = createIndex(indexName).source(indexSource)

    esResource.use { es =>
      es.createIndex(createIndexRequest) *>
        es.getIndex(getIndex(indexName))
    }.asserting { response =>
      val indexResult = response.result(indexName)
      val mappings = indexResult.mappings.properties("a")
      val settings = indexResult.settings("index.number_of_shards")
      mappings.`type`.get shouldBe "text"
      settings shouldBe "1"
    }
  }

  it should "execute a delete index request" in {
    val indexName = genIndex.sample.get
    val indexSource = """{"settings":{"number_of_shards":1},"mappings":{"properties":{"a":{"type":"text"}}}}"""
    val createIndexRequest = createIndex(indexName).source(indexSource)
    val deleteIndexRequest = deleteIndex(indexName)

    esResource.use { es =>
      Task.sequence(
        Seq(
          es.createIndex(createIndexRequest),
          es.deleteIndex(deleteIndexRequest)
        )
      ) *>
      es.getIndex(getIndex(indexName)).map(_.result(indexName)).attempt
    }.asserting { getIndexAttempt =>
      getIndexAttempt.isLeft shouldBe true
      getIndexAttempt.swap.getOrElse(DummyException("failed")) shouldBe a[NoSuchElementException]
    }
  }

  it should "execute a single count request" in {
    val index = genIndex.sample.get
    val updateRequests = Gen.listOfN(100, genUpdateRequest(index)).sample.get
    val countRequest = count(Indexes(index)).query(matchAllQuery())

    esResource.use { es =>
      Task
        .parSequence(updateRequests.map(es.singleUpdate))
        .flatMap(_ => es.refresh(Seq(index)))
        .flatMap(_ => es.singleCount(countRequest))
    }.asserting(_.result.count shouldBe updateRequests.map(_.id).distinct.length)
  }
}
