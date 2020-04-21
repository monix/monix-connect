package cloriko.monix.connect.akka

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source}
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.reactive.{Consumer, Observable}
import org.scalacheck.Gen
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.Future
import scala.concurrent.duration._

class AkkaStreamsImplicitsSpec extends AnyWordSpecLike with ScalaFutures with Matchers {

  implicit val actorSystem = ActorSystem("Akka-Streams-InterOp")
  override implicit val patienceConfig = PatienceConfig(4.seconds, 100.milliseconds)

  s"An akka ${Sink} " should {
    "be extended with the `asConsumer` signature " when {
      "the materialized type is a future integer " in {
        //given
        import AkkaStreams.Implicits._
        val foldSumSink: Sink[Int, Future[Int]] = Sink.fold[Int, Int](0)((acc, num) => acc + num)

        //when
        val (consumer: Consumer[Int, Task[Int]]) = foldSumSink.asConsumer[Int] //extended generic sink
        val f: Task[Int] = Observable.fromIterable(Seq(1, 2, 3)).consumeWith(consumer).runSyncUnsafe()

        //then
        f.runSyncUnsafe() shouldBe 6
      }

      "the materialized type is a future double " in {
        //given
        val foldSumSink: Sink[Double, Future[Seq[Double]]] = Sink.seq[Double]
        import AkkaStreams.Implicits._

        //when
        val consumer: Consumer[Double, Task[Seq[Double]]] = foldSumSink.asConsumer //extended generic sink
        val f: Task[Seq[Double]] = Observable.fromIterable(Seq(1.23, 2.34, 4.56)).consumeWith(consumer).runSyncUnsafe()

        //then
        f.runSyncUnsafe() shouldBe Seq(1.23, 2.34, 4.56)
      }

      "the materialized type is a future string" in {
        //given
        val elem: String = Gen.alphaStr.sample.get
        val sink: Sink[String, Future[String]] = Sink.head[String]
        import AkkaStreams.Implicits._

        //when
        val (consumer: Consumer[String, Task[String]]) = sink.asConsumer //extended generic sink
        val t: Task[String] = Observable.fromIterable(Seq(elem)).consumeWith(consumer).runSyncUnsafe()

        //then
        t.runSyncUnsafe() shouldBe elem
      }

      "the materialized type is a future optional string" in {
        //given
        val sink: Sink[String, Future[Option[String]]] = Sink.headOption[String]
        import AkkaStreams.Implicits._

        //when
        val (consumer: Consumer[String, Task[Option[String]]]) = sink.asConsumer //extended generic sink
        val t: Task[Option[String]] = Observable.fromIterable(Seq("Hello World!")).consumeWith(consumer).runSyncUnsafe()

        //then
        t.runSyncUnsafe() shouldBe Some("Hello World!")
      }
    }
  }

  s"An akka ${Flow} " should {
    "be extended with the `asConsumer` signature " when {
      "the materialized type is a future integer " in {
        //given
        import AkkaStreams.Implicits._
        val foldSumSink: Flow[Int, Int, NotUsed] = Flow[Int].fold[Int](0)((acc, num) => acc + num)

        //when
        val (consumer: Consumer[Int, Task[Int]]) = foldSumSink.asConsumer //extended generic sink
        val f: Task[Int] = Observable.fromIterable(Seq(1, 2, 3)).consumeWith(consumer).runSyncUnsafe()

        //then
        f.runSyncUnsafe() shouldBe 6
      }

      "the materialized type is a future optional string" in {
        //given
        val lastElement: Option[String] = Some("Last element")
        val flow: Flow[Option[String], Option[String], NotUsed] = Flow[Option[String]]
        import AkkaStreams.Implicits._

        //when
        val (consumer: Consumer[Option[String], Task[Option[String]]]) = flow.asConsumer //extended generic sink
        val t: Task[Option[String]] =
          Observable.fromIterable(Seq(None, lastElement)).consumeWith(consumer).runSyncUnsafe()

        //then
        t.runSyncUnsafe() shouldBe lastElement
      }
    }
  }

  s"An akka ${Source} " should {

    "be extended with the `asObservable` signature " when {

      "the output type is an int " in {
        //given
        val elements = 1 until 50
        val source: Source[Int, NotUsed] = Source.fromIterator[Int](() => elements.iterator)
        import AkkaStreams.Implicits._

        //when
        val ob: Observable[Int] = source.asObservable

        //then
        val t: Task[List[Int]] = ob.toListL
        t.runSyncUnsafe() should contain theSameElementsAs elements
      }

      "the output type is an string " in {
        //given
        val elem: String = "Hello world"
        val source: Source[String, NotUsed] = Source.single(elem)
        import AkkaStreams.Implicits._

        //when
        val ob: Observable[String] = source.asObservable.map(_ + "!")

        //then
        val t: Task[String] = ob.headL
        t.runSyncUnsafe() shouldBe (elem + "!")
      }

      "the output type is a pre defined case class " in {
        //given
        case class UserInfo(username: String, password: String)
        val elem: UserInfo = UserInfo("user1", "*****")
        val source: Source[UserInfo, NotUsed] = Source.single(elem)
        import AkkaStreams.Implicits._

        //when
        val ob: Observable[UserInfo] = source.asObservable

        //then
        val t: Task[UserInfo] = ob.headL
        t.runSyncUnsafe() shouldBe elem
      }
    }
  }
}
