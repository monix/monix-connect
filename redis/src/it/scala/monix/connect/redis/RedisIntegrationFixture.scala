package monix.connect.redis

import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import org.scalacheck.Gen

trait RedisIntegrationFixture {
  val redisUrl = "redis://localhost:6379"
  type K = String
  type V = String
  val genRedisKey: Gen[K] = Gen.alphaStr
  val genRedisValue: Gen[V] = Gen.choose(0, 10000).map(_.toString)
  val genRedisValues: Gen[List[V]] = for {
    n      <- Gen.chooseNum(2, 10)
    values <- Gen.listOfN(n, Gen.choose(0, 10000))
  } yield values.map(_.toString)

  val genRedisPair: Gen[(K, V)] = for {
    key <- genRedisKey
    value <- genRedisValue
  } yield (key, value)

  def genRedisPairs: Gen[Map[K, V]] =
    Gen.listOf(genRedisPair).map(_.toMap)

  val genLong: Gen[Long] = Gen.choose[Long](min = -1000L, max = 1000L)
  val genDouble: Gen[Double] = Gen.choose[Double](min = -1000L, max = 1000L)

  implicit val connection: StatefulRedisConnection[String, String] = RedisClient.create(redisUrl).connect()

}
