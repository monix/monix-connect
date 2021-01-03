/*
 * Copyright (c) 2020-2021 by The Monix Connect Project Developers.
 * See the project homepage at: https://connect.monix.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package monix.connect.akka.stream

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

class AkkaStreamsConvertersSpec extends AnyWordSpecLike with ScalaFutures with Matchers {

  implicit val actorSystem: ActorSystem = ActorSystem("Akka-Streams-InterOp")
  override implicit val patienceConfig = PatienceConfig(4.seconds, 100.milliseconds)
  import monix.connect.akka.stream.Converters._

  s"An akka ${Sink} " should {

    "be extended with the `asConsumer` signature " when {

      "the materialized type is a future integer " in {
        //given
        val foldSumSink: Sink[Int, Future[Int]] = Sink.fold[Int, Int](0)((acc, num) => acc + num)

        //when
        val (consumer: Consumer[Int, Int]) = foldSumSink.asConsumer[Int] //extended generic sink
        val t: Task[Int] = Observable.fromIterable(Seq(1, 2, 3)).consumeWith(consumer)

        //then
        t.runSyncUnsafe() shouldBe 6
      }

      "the materialized type is the list of all the elements collected as a future " in {
        //given
        val foldSumSink: Sink[Double, Future[Seq[Double]]] = Sink.seq[Double]

        //when
        val consumer: Consumer[Double, Seq[Double]] = foldSumSink.asConsumer //extended generic sink
        val t: Task[Seq[Double]] = Observable.fromIterable(Seq(1.23, 2.34, 4.56)).consumeWith(consumer)

        //then
        t.runSyncUnsafe() shouldBe Seq(1.23, 2.34, 4.56)
      }

      "the materialized type is the first received string" in {
        //given
        val elem: String = Gen.alphaStr.sample.get
        val sink: Sink[String, Future[String]] = Sink.head[String]

        //when
        val (consumer: Consumer[String, String]) = sink.asConsumer //extended generic sink
        val t: Task[String] = Observable.fromIterable(Seq(elem)).consumeWith(consumer)

        //then
        t.runSyncUnsafe() shouldBe elem
      }

      "the materialized type is a future optional string" in {
        //given
        val sink: Sink[String, Future[Option[String]]] = Sink.headOption[String]

        //when
        val r1: Task[Option[String]] =
          Observable.fromIterable(Seq("Hello World!")).consumeWith(sink.asConsumer)
        val r2: Task[Option[String]] = Observable.empty[String].consumeWith(sink.asConsumer)

        //then
        r1.runSyncUnsafe() shouldBe Some("Hello World!")
        r2.runSyncUnsafe() shouldBe None
      }

      "the input type is different than the materialized one (Int => String)" in {
        //given
        val sink: Sink[Int, Future[String]] = Sink.fold[String, Int]("")((s, i) => s + i.toString)

        //when
        val t: Task[String] = Observable.fromIterable(1 until 10).consumeWith(sink.asConsumer[String])

        //then
        t.runSyncUnsafe() shouldBe "123456789"
      }

    }

  }

  s"An akka ${Flow} " should {

    "be extended with the `asConsumer` signature " when {

      "the materialized type is a future integer " in {
        //given
        val foldSumSink: Flow[Int, Int, NotUsed] = Flow[Int].fold[Int](0)((acc, num) => acc + num)

        //when
        val (consumer: Consumer[Int, Int]) = foldSumSink.asConsumer //extended generic sink
        val t: Task[Int] = Observable.fromIterable(Seq(1, 2, 3)).consumeWith(consumer)

        //then
        t.runSyncUnsafe() shouldBe 6
      }

      "the materialized type is a future optional string" in {
        //given
        val lastElement: Option[String] = Some("Last element")
        val flow: Flow[Option[String], Option[String], NotUsed] = Flow[Option[String]]

        //when
        val (consumer: Consumer[Option[String], Option[String]]) = flow.asConsumer //extended generic sink
        val t: Task[Option[String]] =
          Observable.fromIterable(Seq(None, lastElement)).consumeWith(consumer)

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

        //when
        val ob: Observable[UserInfo] = source.asObservable

        //then
        val t: Task[UserInfo] = ob.headL
        t.runSyncUnsafe() shouldBe elem
      }

    }

    "allow to be consumed directly as shotcut to `asObservable.consumeWith`" in {
      //given
      val elements = 1 until 50
      val source: Source[Int, NotUsed] = Source.fromIterator[Int](() => elements.iterator)

      //when
      val t: Task[List[Int]] = source.consumeWith(Consumer.toList)

      //then
      t.runSyncUnsafe() should contain theSameElementsAs elements
    }

  }

  s"A monix ${Observable} extended with the `asSource` signature" should {

    s"be converted into a ${Source}" when {

      "the input is of integer type and materializes to a seq that contains all elements" in {
        //given
        import monix.connect.akka.stream.Converters._
        val ob = Observable.range(5, 20)

        val f: Future[Seq[Long]] = ob.asSource.runWith(Sink.seq)

        f.futureValue shouldBe (5 until 20)
      }

      "the input is of string type and materializes to foldLeft operator that aggregates all" in {
        //given
        import monix.connect.akka.stream.Converters._
        val elements: Seq[String] = Gen.listOfN(6, Gen.alphaLowerStr).sample.get
        val ob: Observable[String] = Observable.fromIterable(elements)

        val f: Future[String] = ob.asSource.runWith(Sink.fold("") { case (acc, next) => acc ++ next })

        f.futureValue shouldBe elements.mkString
      }

    }

  }

  s"A monix ${Consumer} extended with the `asSink` signature" should {

    s"be converted into a ${Sink}" when {

      "the consumer is returning the first element" in {
        //given
        import monix.connect.akka.stream.Converters._
        val l: List[Int] = Gen.listOfN(100, Gen.choose(1, 1000)).sample.get
        val headConsumer: Consumer.Sync[Int, Int] = Consumer.head[Int]

        //when
        val f: Future[Int] = Source(l).runWith(headConsumer.asSink)

        //then
        f.futureValue shouldBe l.head
      }

      "the consumer is a foldLeft that aggregates all incoming string elements" in {
        //given
        import monix.connect.akka.stream.Converters._
        val l: List[String] = Gen.listOfN(100, Gen.alphaLowerStr).sample.get
        val sink: Sink[String, Future[String]] =
          Consumer.foldLeft("")((acc: String, next: String) => acc ++ next).asSink

        //when
        val f: Future[String] = Source(l).runWith(sink)

        //then
        f.futureValue shouldBe l.mkString
      }

    }

  }
}
