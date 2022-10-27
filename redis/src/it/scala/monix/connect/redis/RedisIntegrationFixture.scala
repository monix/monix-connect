package monix.connect.redis

import cats.effect.Resource
import monix.connect.redis.client.{RedisCmd, RedisConnection, RedisUri}
import monix.connect.redis.domain.VScore
import org.scalacheck.Gen
import monix.connect.redis.test.protobuf.{Person, PersonPk}
import monix.eval.{Task, TaskLike}

trait RedisIntegrationFixture {
  val redisUrl = "redis://localhost:6379"
  type K = String
  type V = String

  //protected val clusterRedisUris = List(7000, 7001, 7002, 7003, 7004, 7005).map(port => RedisUri("127.0.0.1", port))
  protected val clusterRedisUris = List(7000).map(port => RedisUri("127.0.0.1", port))

  val redisUri = RedisUri("localhost", 6379).withDatabase(2000)
  def redis: RedisConnection = Gen.chooseNum(1, 9999)
    .map(dbNum => RedisConnection.standalone(redisUri.withDatabase(dbNum))).sample.get
  def utfConnection: Resource[Task, RedisCmd[String, String]] = redis.connectUtf
  def redisCluster: RedisConnection = Gen.chooseNum(1, 9999)
    .map(dbNum => RedisConnection.cluster(clusterRedisUris.map(_.withDatabase(dbNum)))).sample.get
  def utfClusterConnection: Resource[Task, RedisCmd[String, String]] = redisCluster.connectUtf

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

  implicit val fromGen: TaskLike[Gen] = {
    new TaskLike[Gen] {
      def apply[A](fa: Gen[A]): Task[A] = Task(fa.sample.get)
    }
  }

}
