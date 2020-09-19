/*
 * Copyright (c) 2020-2020 by The Monix Connect Project Developers.
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

package monix.connect.mongodb

import com.mongodb.client.result.{InsertOneResult => MongoInsertOneResult}
import com.mongodb.reactivestreams.client.MongoCollection
import monix.eval.{Coeval, Task}
import monix.execution.Scheduler.Implicits.global
import monix.execution.exceptions.DummyException
import monix.execution.schedulers.TestScheduler
import monix.reactive.Observable
import monix.connect.mongodb.domain.InsertOneResult
import org.mockito.IdiomaticMockito
import org.mockito.MockitoSugar.{times, verify, when}
import org.mongodb.scala.bson.BsonObjectId
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class ObjectPackageSpec
  extends AnyFlatSpecLike with TestFixture with ScalaFutures with Matchers with BeforeAndAfterEach
  with IdiomaticMockito {

  implicit val col: MongoCollection[Employee] = mock[MongoCollection[Employee]]
  implicit val defaultConfig: PatienceConfig = PatienceConfig(5.seconds, 300.milliseconds)

  override def beforeEach() = {
    reset(col)
    super.beforeEach()
  }

  "retryOnFailure" should "accept a Coeval with a function that expects A and return reactivestreams publishers" in {
    //given
    val e = genEmployee.sample.get
    val objectId = BsonObjectId.apply()
    val publisher = Task(MongoInsertOneResult.acknowledged(objectId)).toReactivePublisher(global)
    when(col.insertOne(e)).thenReturn(publisher)

    //when
    val t = retryOnFailure[InsertOneResult](
      Coeval(MongoOp.insertOne(col, e).toReactivePublisher(global)),
      retries = 1,
      timeout = None,
      delayAfterFailure = None)

    //then
    t.runSyncUnsafe() shouldBe Some(InsertOneResult(Some(objectId.getValue.toString), true))
  }

  it should "recover from empty publisher as many times as passed" in {
    //given
    val s = TestScheduler()
    val attempts = 2
    val e = genEmployee.sample.get
    val emptyPub = Observable.empty[MongoInsertOneResult].toReactivePublisher(s)
    val objectId = BsonObjectId.apply()
    val insertOneResult = MongoInsertOneResult.acknowledged(objectId)
    val successPub = Task(insertOneResult).toReactivePublisher(s)
    when(col.insertOne(e)).thenReturn(emptyPub, emptyPub, successPub)

    //when
    val f =
      retryOnFailure[MongoInsertOneResult](Coeval(col.insertOne(e)), attempts, timeout = None, delayAfterFailure = None)
        .runToFuture(s)
    /**
      * in this case we couldn't perform the test with MongoOp since it already handles empty publishers
      * so in that case it would not retry.
      */

    //then
    s.tick(1.second)
    f.value.get shouldBe util.Success(Some(insertOneResult))
    verify(col, times(attempts + 1)).insertOne(e)
  }

  it should "recover from failures as many times as indicated" in {
    //given
    val s = TestScheduler()
    val attempts = 2
    val e = genEmployee.sample.get
    val ex = DummyException("Kaboom!")
    val failedPub = Task.raiseError[MongoInsertOneResult](ex).toReactivePublisher(s)
    val objectId = BsonObjectId.apply()
    val insertOneResult = MongoInsertOneResult.acknowledged(objectId)
    val successPub = Task(insertOneResult).toReactivePublisher(s)
    when(col.insertOne(e)).thenReturn(failedPub, failedPub, successPub)

    //when
    val f = retryOnFailure[InsertOneResult](
      Coeval(MongoOp.insertOne(col, e).toReactivePublisher(s)),
      attempts,
      timeout = None,
      delayAfterFailure = None).runToFuture(s)

    //then
    s.tick(1.second)
    f.value.get shouldBe util.Success(Some(InsertOneResult(Some(objectId.getValue.toString), true)))
    verify(col, times(attempts + 1)).insertOne(e)
  }

  it should "recover from either an empty publisher or a failed one" in {
    //given
    val s = TestScheduler()
    val attempts = 2
    val e = genEmployee.sample.get
    val ex = DummyException("Kaboom!")
    val emptyPub = Observable.empty[MongoInsertOneResult].toReactivePublisher(s)
    val failedPub = Task.raiseError[MongoInsertOneResult](ex).toReactivePublisher(s)
    val objectId = BsonObjectId.apply()
    val insertOneResult = MongoInsertOneResult.acknowledged(objectId)
    val successPub = Task(insertOneResult).toReactivePublisher(s)
    when(col.insertOne(e)).thenReturn(emptyPub, failedPub, successPub)

    //when
    val f =
      retryOnFailure[MongoInsertOneResult](Coeval(col.insertOne(e)), attempts, timeout = None, delayAfterFailure = None)
        .runToFuture(s)

    //then
    s.tick(1.second)
    f.value.get shouldBe util.Success(Some(insertOneResult))
    verify(col, times(attempts + 1)).insertOne(e)
  }

  it should "fail when the failures exceeded the number of retries" in {
    //given
    val s = TestScheduler()
    val attempts = 3
    val e = genEmployee.sample.get
    val ex = DummyException("Kaboom!")
    val failedPub = Task.raiseError[MongoInsertOneResult](ex).toReactivePublisher(s)
    when(col.insertOne(e)).thenReturn(failedPub)

    //when
    val f =
      retryOnFailure[MongoInsertOneResult](Coeval(col.insertOne(e)), attempts, timeout = None, delayAfterFailure = None)
        .runToFuture(s)

    //then
    s.tick(1.second)
    f.value.get.isFailure shouldBe true
    verify(col, times(attempts + 1)).insertOne(e)
  }

  it should "fail when empty operations exceeded the number of retries" in {
    //given
    val s = TestScheduler()
    val attempts = 3
    val e = genEmployee.sample.get
    val ex = DummyException("Kaboom!")
    val emptyPub = Observable.empty[MongoInsertOneResult].toReactivePublisher(s)
    val failedPub = Task.raiseError[MongoInsertOneResult](ex).toReactivePublisher(s)
    when(col.insertOne(e)).thenReturn(emptyPub, emptyPub)

    //when
    val f =
      retryOnFailure[MongoInsertOneResult](Coeval(col.insertOne(e)), attempts, timeout = None, delayAfterFailure = None)
        .runToFuture(s)

    //then
    s.tick(1.second)
    f.value.get shouldBe util.Success(None)
    verify(col, times(attempts + 1)).insertOne(e)
  }

  it should "fail when the failures and empty operations exceeded the number of retries" in {
    //given
    val s = TestScheduler()
    val attempts = 5
    val e = genEmployee.sample.get
    val ex = DummyException("Kaboom!")
    val emptyPub = Observable.empty[MongoInsertOneResult].toReactivePublisher(s)
    val failedPub = Task.raiseError[MongoInsertOneResult](ex).toReactivePublisher(s)
    when(col.insertOne(e)).thenReturn(emptyPub, failedPub)

    //when
    val f =
      retryOnFailure[MongoInsertOneResult](Coeval(col.insertOne(e)), attempts, timeout = None, delayAfterFailure = None)
        .runToFuture(s)

    //then
    s.tick(1.second)
    f.value.get.isFailure shouldBe true
    verify(col, times(attempts + 1)).insertOne(e)
  }

  it should "fail when the operation exceeded the specified timeout" in {
    //given
    val s = TestScheduler()
    val e = genEmployee.sample.get
    val objectId = BsonObjectId.apply()
    val insertOneResult = MongoInsertOneResult.acknowledged(objectId)
    val pub = Task(insertOneResult).delayResult(150.milliseconds).toReactivePublisher(s)
    when(col.insertOne(e)).thenReturn(pub)

    //when
    val f = retryOnFailure[InsertOneResult](
      Coeval(
        MongoOp
          .insertOne(col, e)
          .toReactivePublisher(s)),
      0,
      timeout = Some(50.milliseconds),
      delayAfterFailure = None)
      .runToFuture(s)

    //then
    s.tick(1.second)
    f.value.get.isFailure shouldBe true
    verify(col).insertOne(e)
  }

  it should "retry by timeout n times and succeed" in {
    //given
    val s = TestScheduler()
    val n = 2
    val e = genEmployee.sample.get
    val objectId = BsonObjectId.apply()
    val insertOneResult = MongoInsertOneResult.acknowledged(objectId)
    val delayedPub = Task(insertOneResult).delayExecution(100.milliseconds).toReactivePublisher(s)
    val nonDelayedPub = Task(insertOneResult).toReactivePublisher(s)
    when(col.insertOne(e)).thenReturn(delayedPub, delayedPub, nonDelayedPub)

    //when
    val f = retryOnFailure[InsertOneResult](
      Coeval(
        MongoOp
          .insertOne(col, e)
          .toReactivePublisher(s)),
      n,
      timeout = Some(50.milliseconds),
      delayAfterFailure = Option.empty)
      .runToFuture(s)

    //then
    s.tick(1.second)
    f.value.get shouldBe util.Success(Some(InsertOneResult(Some(objectId.getValue.toString), true)))
    verify(col, times(n + 1)).insertOne(e)
  }

}
