package monix.connect.es

import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.requests.delete.DeleteByIdRequest
import com.sksamuel.elastic4s.requests.get.GetResponse
import com.sksamuel.elastic4s.requests.indexes.IndexRequest
import com.sksamuel.elastic4s.requests.update.UpdateRequest
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties, RequestFailure, RequestSuccess}
import monix.eval.Task
import org.scalacheck.Gen

trait Fixture {
  import com.sksamuel.elastic4s.ElasticDsl._
  implicit val client: ElasticClient = ElasticClient(JavaClient(ElasticProperties("http://localhost:9200")))
  def genIndex: Gen[String] = Gen.nonEmptyListOf(Gen.alphaLowerChar).map(_.mkString.take(10))
  def getDocString(a: String, b: String) = s"""{"a":"$a","b":"$b"}"""
  def genDoc: Gen[String] =
    for {
      a <- Gen.nonEmptyListOf(Gen.alphaLowerChar).map(_.mkString.take(10))
      b <- Gen.nonEmptyListOf(Gen.alphaLowerChar).map(_.mkString.take(10))
    } yield getDocString(a, b)

  def genUpdateRequest: Gen[UpdateRequest] =
    for {
      index <- genIndex
      id    <- Gen.nonEmptyListOf(Gen.alphaLowerChar).map(_.mkString.take(10))
      doc   <- genDoc
    } yield updateById(index, id).docAsUpsert(doc)

  def genUpdateRequest(index: String): Gen[UpdateRequest] =
    for {
      id    <- Gen.nonEmptyListOf(Gen.alphaLowerChar).map(_.mkString.take(10))
      doc   <- genDoc
    } yield updateById(index, id).docAsUpsert(doc)

  def genDeleteRequest: Gen[DeleteByIdRequest] =
    for {
      index <- genIndex
      id    <- Gen.nonEmptyListOf(Gen.alphaLowerChar).map(_.mkString.take(10))
    } yield deleteById(index, id)

  def genIndexRequest: Gen[IndexRequest] =
    for {
      index <- genIndex
      id    <- Gen.nonEmptyListOf(Gen.alphaLowerChar).map(_.mkString.take(10))
      doc   <- genDoc
    } yield indexInto(index).id(id).doc(doc)

  def getById(index: String, id: String): Task[GetResponse] = {
    client.execute(get(index, id)).map {
      case RequestSuccess(_, _, _, result) =>
        result
      case RequestFailure(_, _, _, error) =>
        throw new RuntimeException(error.toString)
    }
  }
}
