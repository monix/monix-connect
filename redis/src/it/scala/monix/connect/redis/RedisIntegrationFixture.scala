package monix.connect.redis

import monix.connect.redis.client.Redis
import org.scalacheck.Gen
import monix.connect.redis.test.protobuf.{Person, PersonPk}

trait RedisIntegrationFixture {
  val redisUrl = "redis://localhost:6379"
  type K = String
  type V = String

  val redisClient = Redis.connect(redisUrl)
  val genRedisKey: Gen[K] = Gen.identifier.map(_.take(10))
  val genRedisValue: Gen[V] = Gen.choose(0, 10000).map(_.toString)
  val genRedisValues: Gen[List[V]] = for {
    n      <- Gen.chooseNum(2, 10)
    values <- Gen.listOfN(n, Gen.choose(0, 10000))
  } yield values.map(_.toString)

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
