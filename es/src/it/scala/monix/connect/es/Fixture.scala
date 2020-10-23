package monix.connect.es

import com.sksamuel.elastic4s.IndexAndType
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.get.GetResponse
import com.sksamuel.elastic4s.http.{ElasticClient, ElasticProperties, RequestFailure, RequestSuccess}
import com.sksamuel.elastic4s.update.UpdateRequest
import monix.eval.Task
import org.scalacheck.Gen

trait Fixture {
  implicit val elasticClient: ElasticClient = ElasticClient(ElasticProperties(s"http://localhost:9200"))
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
    } yield update(id).in(IndexAndType(index, "_doc")).docAsUpsert(doc)

  def getById(index: String, id: String): Task[GetResponse] = {
    elasticClient.execute(get(id).from(index, "_doc")).map {
      case RequestSuccess(_, _, _, result) =>
        result
      case RequestFailure(_, _, _, error) =>
        throw new RuntimeException(error.toString)
    }
  }
}
