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
import monix.connect.mongodb.domain.{InsertOneResult, RetryStrategy}
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

  private[this] implicit val col: MongoCollection[Employee] = mock[MongoCollection[Employee]]
  private[this] implicit val defaultConfig: PatienceConfig = PatienceConfig(5.seconds, 300.milliseconds)
  private[this] val objectId = BsonObjectId.apply()

  override def beforeEach() = {
    reset(col)
    super.beforeEach()
  }

  "deprecated retryOnFailure" should "accept a Coeval with a function that expects A and return reactivestreams publishers" in {
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

  it should "recover from failures as many times as indicated" in {
    //given
    val s = TestScheduler()
    val attempts = 2
    val e = genEmployee.sample.get
    val ex = DummyException("Kaboom!")
    val failedPub = Task.raiseError[MongoInsertOneResult](ex).toReactivePublisher(s)
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

  it should "recovers from a failure" in {
    //given
    val s = TestScheduler()
    val attempts = 1
    val e = genEmployee.sample.get
    val ex = DummyException("Kaboom!")
    val failedPub = Task.raiseError[MongoInsertOneResult](ex).toReactivePublisher(s)
    val insertOneResult = MongoInsertOneResult.acknowledged(objectId)
    val successPub = Task(insertOneResult).toReactivePublisher(s)
    when(col.insertOne(e)).thenReturn(failedPub, successPub)

    //when
    val f =
      retryOnFailure[MongoInsertOneResult](Coeval(col.insertOne(e)), attempts, timeout = None, delayAfterFailure = None)
        .runToFuture(s)

    //then
    s.tick(1.second)
    f.value.get shouldBe util.Success(Some(insertOneResult))
    verify(col, times(attempts + 1)).insertOne(e)
  }

  it should "fail when the failures exceeds the number of retries" in {
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

  "new retryOnFailure" should "expect a non-strict function from `A` to `Publisher`" in {
    //given
    val retryStrategy = RetryStrategy(1, 100.millis)
    val employee = genEmployee.sample.get
    val publisher = Task(MongoInsertOneResult.acknowledged(objectId)).toReactivePublisher(global)
    when(col.insertOne(employee)).thenReturn(publisher)

    //when
    val t = retryOnFailure(MongoSingle.insertOne(col, employee).toReactivePublisher(global), retryStrategy)

    //then
    t.runSyncUnsafe() shouldBe Some(InsertOneResult(Some(objectId.getValue.toString), true))
  }

  it should "recover from failures as many times as indicated" in {
    //given
    val retryStrategy = RetryStrategy(2, 100.millis)
    val s = TestScheduler()
    val employee = genEmployee.sample.get
    val ex = DummyException("Kaboom!")
    val failedPub = Task.raiseError[MongoInsertOneResult](ex).toReactivePublisher(s)
    val insertOneResult = MongoInsertOneResult.acknowledged(objectId)
    val successPub = Task(insertOneResult).toReactivePublisher(s)

    //when
    when(col.insertOne(employee)).thenReturn(failedPub, failedPub, successPub)

    //and
    val f = retryOnFailure(MongoSingle.insertOne(col, employee).toReactivePublisher(s), retryStrategy).runToFuture(s)

    //then
    s.tick(1.second)
    f.value.get shouldBe util.Success(Some(InsertOneResult(Some(objectId.getValue.toString), true)))
    verify(col, times(retryStrategy.attempts + 1)).insertOne(employee)
  }

  it should "recovers from a failure" in {
    //given
    val retryStrategy = RetryStrategy(1, 100.millis)
    val s = TestScheduler()
    val employee = genEmployee.sample.get
    val ex = DummyException("Kaboom!")
    val failedPub = Task.raiseError[MongoInsertOneResult](ex).toReactivePublisher(s)
    val insertOneResult = MongoInsertOneResult.acknowledged(objectId)
    val successPub = Task(insertOneResult).toReactivePublisher(s)
    when(col.insertOne(employee)).thenReturn(failedPub, successPub)

    //when
    val f = retryOnFailure(col.insertOne(employee), retryStrategy)
      .runToFuture(s)

    //then
    s.tick(1.second)
    f.value.get shouldBe util.Success(Some(insertOneResult))
    verify(col, times(retryStrategy.attempts + 1)).insertOne(employee)
  }

  it should "fail when the failures exceeds the number of retries" in {
    //given
    val retryStrategy = RetryStrategy(3, 100.millis)
    val s = TestScheduler()
    val employee = genEmployee.sample.get
    val ex = DummyException("Kaboom!")
    val failedPub = Task.raiseError[MongoInsertOneResult](ex).toReactivePublisher(s)
    when(col.insertOne(employee)).thenReturn(failedPub)

    //when
    val f = retryOnFailure(col.insertOne(employee), retryStrategy)
      .runToFuture(s)

    //then
    s.tick(1.second)
    f.value.get.isFailure shouldBe true
    verify(col, times(retryStrategy.attempts + 1)).insertOne(employee)
  }

}
