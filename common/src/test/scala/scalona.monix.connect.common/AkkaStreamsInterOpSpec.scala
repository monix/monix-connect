package scalona.monix.connect.common

import org.scalatest.flatspec.AnyFlatSpec
import AkkaStreamsInterOp._
import StreamTransformer._
import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.reactive.{Consumer, Observable}
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Future
import concurrent.duration._

class AkkaStreamsInterOpSpec extends AnyFlatSpec with ScalaFutures {

  implicit val actorSystem = ActorSystem("test")
  override implicit val patienceConfig = PatienceConfig(4.seconds, 100.milliseconds)

  "Akka sink" should "be correctly converted to s3sink" in {
    val foldSumSink: Sink[Int, Future[Int]] = Sink.fold[Int, Int](0)((acc, num) => acc + num)
    val (consumer: Consumer[Int, Task[Int]]) = foldSumSink.asConsumer

    val f: Task[Int] = Observable.fromIterable(Seq(1, 2, 3)).consumeWith(consumer).runSyncUnsafe()

    println("Value 1 " + f.runSyncUnsafe())
  }

  it should "bde correctly converted to s3sink" in {
    val foldSumSink: Sink[String, Future[String]] = Sink.head[String]
    val consumer: Consumer[String, Task[String]] = foldSumSink.asConsumer

    val f: Task[String] = Observable.fromIterable(Seq("hello", "world")).consumeWith(consumer).runSyncUnsafe()


    println("Value 2" + f.runSyncUnsafe())
  }
}
