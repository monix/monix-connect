package monix.connect.redis

import io.lettuce.core.pubsub.api.reactive.PatternMessage
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import monix.testing.scalatest.MonixTaskSpec
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, BeforeAndAfterAll, BeforeAndAfterEach}

import scala.concurrent.duration._

class PubSubCommandsSuite
  extends AsyncFlatSpec with MonixTaskSpec with RedisIntegrationFixture with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  override implicit val scheduler: Scheduler = Scheduler.io("string-commands-suite")

  "Pub/sub" should "subscribe to a single channel and receive its events" in {
    val channel: K = "channels"
    val v1: String = genRedisValue.sample.get
    val v2: String = genRedisValue.sample.get

    utfClusterConnection.use[Task, Assertion] { subscribeCmd =>
      utfClusterConnection.use[Task, Assertion] { publishCmd =>
      val receive = subscribeCmd.pubSub.observeChannels.map(_.getMessage).timeoutOnSlowUpstreamTo(4.seconds, Observable.empty).toListL
        val publish = subscribeCmd.pubSub.subscribe(channel) >>
          Task.sleep(3.second) >> publishCmd.pubSub.publish(channel, v1) >>
          publishCmd.pubSub.publish(channel, v2)

        Task.parZip2(receive, publish).asserting { case (received, _) =>
          received shouldBe List(v1, v2)
        }
      }
    }
  }

  it should "subscribes and receives events from multiple channels" in {
    val channel1: K = genRedisValue.sample.get
    val channel2: K = genRedisValue.sample.get

    val v1: String = genRedisValue.sample.get
    val v2: String = genRedisValue.sample.get
    val v3: String = genRedisValue.sample.get

    utfConnection.use[Task, Assertion] { subscribeCmd =>
      utfConnection.use[Task, Assertion] { publishCmd =>
        val receive = subscribeCmd.pubSub.observeChannels.map(_.getMessage).timeoutOnSlowUpstreamTo(4.seconds, Observable.empty).toListL
        val publish = subscribeCmd.pubSub.subscribe(List(channel1, channel2)) >>
          Task.sleep(3.second) >> publishCmd.pubSub.publish(channel1, v1) >>
          publishCmd.pubSub.publish(channel2, v2) >>
        publishCmd.pubSub.publish(channel2, v3)

        Task.parZip2(receive, publish).asserting { case (received, _) =>
          received shouldBe List(v1, v2, v3)
        }
      }
    }
  }

  it should "subscribe to a single channel" in {
    val channel1: K = genRedisValue.sample.get
    val channel2: K = genRedisValue.sample.get

    val v1: String = genRedisValue.sample.get
    val v2: String = genRedisValue.sample.get
    val v3: String = genRedisValue.sample.get
    utfConnection.use[Task, Assertion] { subscribeCmd =>
      utfConnection.use[Task, Assertion] { publishCmd =>
        val receiveFromChannel1 = subscribeCmd.pubSub.observeChannel(channel1).timeoutOnSlowUpstreamTo(4.seconds, Observable.empty).toListL
        val publish = subscribeCmd.pubSub.subscribe(List(channel1, channel2)) >>
          Task.sleep(3.second) >> publishCmd.pubSub.publish(channel1, v1) >>
          publishCmd.pubSub.publish(channel2, v2) >>
          publishCmd.pubSub.publish(channel2, v3)
        Task.parZip2(receiveFromChannel1, publish).asserting { case (received, _) =>
          received shouldBe List(v1) //only messages from channel 1
        }
      }
    }
  }

  it should "subscribe and receives from two different channels separately" in {
    val channel1: K = genRedisValue.sample.get
    val channel2: K = genRedisValue.sample.get

    val v1: String = genRedisValue.sample.get
    val v2: String = genRedisValue.sample.get
    val v3: String = genRedisValue.sample.get
    utfConnection.use[Task, Assertion] { subscribeCmd =>
      utfConnection.use[Task, Assertion] { publishCmd =>
        val receiveFromChannel1 = subscribeCmd.pubSub.observeChannel(channel1).timeoutOnSlowUpstreamTo(4.seconds, Observable.empty).toListL
        val receiveFromChannel2 = subscribeCmd.pubSub.observeChannel(channel2).timeoutOnSlowUpstreamTo(4.seconds, Observable.empty).toListL
        val publish = subscribeCmd.pubSub.subscribe(List(channel1, channel2)) >>
          Task.sleep(3.second) >> publishCmd.pubSub.publish(channel1, v1) >>
          publishCmd.pubSub.publish(channel2, v2) >>
          publishCmd.pubSub.publish(channel2, v3)

        Task.parZip3(receiveFromChannel1, receiveFromChannel2, publish).asserting { case (receivedFromChannel1, receivedFromChannel2, _) =>
          receivedFromChannel1 shouldBe List(v1)
          receivedFromChannel2 shouldBe List(v2, v3)
        }
      }
    }
  }

  "pSubscribe" should "subscribe to a pattern and receive its events" in {
    val pattern: K = "channel*"
    val channelPattern: K = "channel1"
    val nonChannelPattern: K = "chann"
    val v1: String = genRedisValue.sample.get
    val v2: String = genRedisValue.sample.get

    utfClusterConnection.use[Task, Assertion] { subscribeCmd =>
      utfClusterConnection.use[Task, Assertion] { publishCmd =>
        val receive = subscribeCmd.pubSub.observePatterns.map(_.getMessage).timeoutOnSlowUpstreamTo(4.seconds, Observable.empty).toListL
        val publish = subscribeCmd.pubSub.pSubscribe(pattern) >>
          Task.sleep(3.second) >> publishCmd.pubSub.publish(channelPattern, v1) >>
          publishCmd.pubSub.publish(nonChannelPattern, v2)

        Task.parZip2(receive, publish).asserting { case (received, _) =>
          received shouldBe List(v1)
        }
      }
    }
  }

  it should "subscribes and receives events from multiple patterns and channels" in {

    val pattern1 = "channel/*/1".r.toString
    val pattern2 = "channel/*/2".r.toString
    val channel1: K = "channel/message/1"
    val channel2: K = "channel/message/2"

    val v1: String = genRedisValue.sample.get
    val v2: String = genRedisValue.sample.get
    val v3: String = genRedisValue.sample.get

    utfConnection.use[Task, Assertion] { subscribeCmd =>
      utfConnection.use[Task, Assertion] { publishCmd =>
        val receiveAll = subscribeCmd.pubSub.observePatterns.timeoutOnSlowUpstreamTo(4.seconds, Observable.empty).toListL
        val receivePattern1 = subscribeCmd.pubSub.observePattern(pattern1).timeoutOnSlowUpstreamTo(4.seconds, Observable.empty).toListL
        val receivePattern2 = subscribeCmd.pubSub.observePattern(pattern2).timeoutOnSlowUpstreamTo(4.seconds, Observable.empty).toListL

        val publish = subscribeCmd.pubSub.pSubscribe(List(pattern1, pattern2)) >>
          Task.sleep(3.second) >> publishCmd.pubSub.publish(channel1, v1) >>
          publishCmd.pubSub.publish(channel2, v2) >>
          publishCmd.pubSub.publish(channel2, v3)

        Task.parZip4(receiveAll, receivePattern1, receivePattern2, publish).asserting { case (all, p1, p2, _) =>
          all.map(_.getMessage) shouldBe List(v1, v2, v3)

          Option(p1.head).map(p => (p.getPattern, p.getChannel, p.getMessage)) shouldBe Some(pattern1, channel1, v1)
          p2.map(p => (p.getPattern, p.getChannel, p.getMessage)) shouldBe List((pattern2, channel2, v2), (pattern2, channel2, v3))
        }
      }
    }
  }

  it should "support standalone connections" in {
    val channel: K = "channels"
    val v1: String = genRedisValue.sample.get
    val v2: String = genRedisValue.sample.get

    utfConnection.use[Task, Assertion] { subscribeCmd =>
      utfConnection.use[Task, Assertion] { publishCmd =>
        val receive = subscribeCmd.pubSub.observeChannels.map(_.getMessage).timeoutOnSlowUpstreamTo(4.seconds, Observable.empty).toListL
        val publish = subscribeCmd.pubSub.subscribe(channel) >>
          Task.sleep(3.second) >> publishCmd.pubSub.publish(channel, v1) >>
          publishCmd.pubSub.publish(channel, v2)

        Task.parZip2(receive, publish).asserting { case (received, _) =>
          received shouldBe List(v1, v2)
        }
      }
    }
  }
}
