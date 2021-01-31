package monix.connect.redis

import monix.connect.redis.client.{Codec, Redis}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.ByteBuffer
import scala.concurrent.Future

class CodecSuite extends AnyFlatSpec with RedisIntegrationFixture with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with Eventually {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(4.seconds, 100.milliseconds)

  s"A Codec" should "encode and decode int values" in {
    //given
    val key: K = genRedisKey.sample.get
    val field: K = genRedisKey.sample.get
    implicit val keyCodec = Codec.forKey[Int](i => ByteBuffer.wrap(i.toString.toArray))(bytes => bytes.toString.toInt)
    implicit val valueCodec = Codec.forValue[Int](i => ByteBuffer.wrap(i.toString.toArray))(bytes => bytes.toString.toInt)

    //when
    val f: Future[String] = Redis.connectWithCodec(redisUrl).use(_.hash.hGet(key, field)).runToFuture(global)

    //then
    eventually {
      f.value.get.isFailure shouldBe true
      f.value.get.failed.get shouldBe a[NoSuchElementException]
    }
  }


}
