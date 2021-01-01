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

import com.mongodb.reactivestreams.client.MongoCollection
import monix.eval.Task
import monix.execution.exceptions.DummyException
import monix.execution.schedulers.TestScheduler
import monix.reactive.Observable
import monix.connect.mongodb.domain.{DefaultInsertOneOptions, RetryStrategy}
import org.mockito.IdiomaticMockito
import org.mockito.MockitoSugar.{times, verify, when}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import com.mongodb.client.result.{InsertOneResult => MongoInsertOneResult}
import org.mongodb.scala.bson.BsonObjectId

import scala.concurrent.duration._

class MongoSinkSpec
  extends AnyFlatSpecLike with TestFixture with ScalaFutures with Matchers with BeforeAndAfterEach
  with IdiomaticMockito {

  implicit val col: MongoCollection[Employee] = mock[MongoCollection[Employee]]
  implicit val defaultConfig: PatienceConfig = PatienceConfig(5.seconds, 300.milliseconds)

  override def beforeEach() = {
    reset(col)
    super.beforeEach()
  }

  s"${MongoSink}" should "retry when the underlying publisher signaled error or timeout" in {
    //given
    val retryStrategy@RetryStrategy(retries, backoffDelay) = RetryStrategy(3, 200.millis)
    val s = TestScheduler()
    val e1 = genEmployee.sample.get
    val e2 = genEmployee.sample.get
    val objectId = BsonObjectId.apply()
    val insertOneResult = MongoInsertOneResult.acknowledged(objectId)
    val delayedPub = Task(insertOneResult).delayResult(500.millis).toReactivePublisher(s)
    val emptyPub = Observable.empty[MongoInsertOneResult].toReactivePublisher(s)
    val failedPub = Task.raiseError[MongoInsertOneResult](DummyException("Insert one failed")).toReactivePublisher(s)
    val successPub = Task(MongoInsertOneResult.unacknowledged()).toReactivePublisher(s)

    //when
    when(col.insertOne(e1, DefaultInsertOneOptions)).thenReturn(delayedPub, emptyPub, failedPub, successPub)
    when(col.insertOne(e2, DefaultInsertOneOptions)).thenReturn(failedPub, successPub)

    //and
    val sink = MongoSink.insertOne(col, DefaultInsertOneOptions, retryStrategy)
    val f = Observable.from(List(e1, e2)).consumeWith(sink).runToFuture(s)

    //then
    s.tick(1.second)
    f.value.get shouldBe util.Success(())
    verify(col, times(retries + 1)).insertOne(e1, DefaultInsertOneOptions)
    verify(col, times(2)).insertOne(e2, DefaultInsertOneOptions)
  }

  it should "signals on error when the failures exceeded the number of retries" in {
    //given
    val retryStrategy@RetryStrategy(retries, backoffDelay) = RetryStrategy(3, 200.millis)
    val s = TestScheduler()
    val e1 = genEmployee.sample.get
    val ex = DummyException("Insert one failed")
    val delayedPub = Task(MongoInsertOneResult.unacknowledged()).delayResult(500.millis).toReactivePublisher(s)
    val emptyPub = Observable.empty[MongoInsertOneResult].toReactivePublisher(s)
    val failedPub = Task.raiseError[MongoInsertOneResult](DummyException("Insert one failed")).toReactivePublisher(s)
    when(col.insertOne(e1, DefaultInsertOneOptions)).thenReturn(delayedPub, emptyPub, failedPub)

    //when

    val sink = MongoSink.insertOne(col, DefaultInsertOneOptions, retryStrategy)
    val f = Observable.now(e1).consumeWith(sink).runToFuture(s)

    //then
    s.tick(1.second)
    f.value.get shouldBe util.Failure(ex)
    verify(col, times(retries + 1)).insertOne(e1, DefaultInsertOneOptions)
  }

}
