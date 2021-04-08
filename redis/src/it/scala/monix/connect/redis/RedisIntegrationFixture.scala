package monix.connect.redis

import monix.connect.redis.client.{RedisConnection, RedisUri}
import monix.connect.redis.domain.VScore
import org.scalacheck.Gen
import monix.connect.redis.test.protobuf.{Person, PersonPk}

trait RedisIntegrationFixture {
  val redisUrl = "redis://localhost:6379"
  type K = String
  type V = String

  val redisUri = RedisUri("localhost", 6379)
  val utfConnection = RedisConnection.standalone(RedisUri("localhost", 6379)).connectUtf

  val genRedisKey: Gen[K] = Gen.identifier.map(_.take(30))
  val genRedisValue: Gen[V] = Gen.choose(0, 10000).map(_.toString)

  val genKv: Gen[(K,V)] = for {
    key <- genRedisKey
    value <- genRedisValue
  } yield (key, value)

  val genRedisValues: Gen[List[V]] = for {
    n      <- Gen.chooseNum(2, 10)
    values <- Gen.listOfN(n, Gen.choose(0, 10000))
  } yield values.map(_.toString)

  protected val genVScore: Gen[VScore[V]] = {
    for {
      member <- Gen.identifier.map(_.take(30))
      score <- Gen.choose[Double](1, 90000)
    } yield VScore(Some(member), score)
  }

  protected def genVScoreWithRange(lower: Int, upper: Int): Gen[VScore[V]] = {
    for {
      v <- genRedisValue
      score <- Gen.choose[Double](lower, upper)
    } yield VScore(Some(v), score)
  }

  protected def genVScore(score: Double): Gen[VScore[V]] = {
    for {
      v <- genRedisValue
    } yield VScore(v, score)
  }

  val genPerson: Gen[Person] = {
    for {
      age <- Gen.chooseNum(1, 100)
      name <- Gen.identifier
      hobbies <- Gen.listOfN(10, Gen.identifier)
    } yield Person( name, age, hobbies)
  }

  val genPersonPk: Gen[PersonPk] = {
    for {
      id <- Gen.identifier
    } yield PersonPk(id)
  }

}
