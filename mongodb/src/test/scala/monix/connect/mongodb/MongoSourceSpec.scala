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

package monix.connect.mongodb

import com.mongodb.client.model.{Filters, Updates}
import com.mongodb.reactivestreams.client.MongoCollection
import monix.eval.Task
import monix.connect.mongodb.domain.{
  DefaultCountOptions,
  DefaultFindOneAndDeleteOptions,
  DefaultFindOneAndReplaceOptions,
  DefaultFindOneAndUpdateOptions,
  RetryStrategy
}
import monix.execution.Scheduler.Implicits.global
import monix.execution.exceptions.DummyException
import monix.execution.schedulers.TestScheduler
import monix.reactive.Observable
import org.mockito.IdiomaticMockito
import org.mockito.MockitoSugar.{times, verify, when}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._
import scala.util.Failure

//todo unit tests for count, aggregate and distinct
class MongoSourceSpec
  extends AnyFlatSpecLike with TestFixture with ScalaFutures with Matchers with BeforeAndAfterEach
  with IdiomaticMockito {

  implicit val col: MongoCollection[Employee] = mock[MongoCollection[Employee]]
  implicit val defaultConfig: PatienceConfig = PatienceConfig(5.seconds, 300.milliseconds)

  override def beforeEach() = {
    reset(col)
    super.beforeEach()
  }

  "countAll" should "count all" in {
    //given
    val finalCount: java.lang.Long = 1L
    when(col.countDocuments()).thenReturn(Task(finalCount).toReactivePublisher(global))

    //when
    val nElements = MongoSource.countAll(col).runSyncUnsafe()

    //then
    nElements shouldBe finalCount
  }

  it should "perform a filtered count" in {
    //given
    val finalCount: java.lang.Long = 100L
    val filter = Filters.lte("age", 22)
    when(col.countDocuments(filter)).thenReturn(Task(finalCount).toReactivePublisher(global))

    //when
    val nElements = MongoSource.count(col, filter).runSyncUnsafe()

    //then
    nElements shouldBe finalCount
  }

  it should "perform a filtered count with filter and options" in {
    //given
    val retryStrategy = RetryStrategy(attempts = 1)
    val s = TestScheduler()
    val finalCount: java.lang.Long = 15L
    val filter = Filters.lte("age", 22)
    val failedPub = Task.raiseError[java.lang.Long](DummyException("Count one failed")).toReactivePublisher(s)
    val successPub = Task(finalCount).toReactivePublisher(s)
    when(col.countDocuments(filter, DefaultCountOptions)).thenReturn(failedPub, successPub)

    //when
    val f = MongoSource
      .count(col, filter, countOptions = DefaultCountOptions, retryStrategy)
      .runToFuture(s)

    //then
    s.tick(1.second)
    whenReady(f) { nElements =>
      verify(col, times(2)).countDocuments(filter, DefaultCountOptions)
      nElements shouldBe finalCount
    }
  }

  it should "return -1 whenever the operation publisher did not emitted any elements" in {
    //given
    when(col.countDocuments()).thenReturn(Observable.empty.toReactivePublisher(global))

    //when
    val nElements = MongoSource.countAll(col).runSyncUnsafe()

    //then
    nElements shouldBe -1L
  }

  it should "return a failed task whenever the underlying publisher signaled error" in {
    //given
    val s = TestScheduler()
    val exception = DummyException("Mongo count failure")
    val publisher = Task.raiseError[java.lang.Long](exception).toReactivePublisher(s)
    when(col.countDocuments()).thenReturn(publisher)

    //when
    val f = MongoSource.countAll(col).runToFuture(s)

    //then
    s.tick(1.second)
    f.value shouldBe Some(Failure(exception))
  }

  "findOneAndDelete" should "return the same element that was deleted" in {
    //given
    val s = TestScheduler()
    val filter = Filters.eq("name", "Romanian")
    val employee = genEmployee.sample.get
    val publisher = Task(employee).toReactivePublisher(s)
    when(col.findOneAndDelete(filter)).thenReturn(publisher)

    //when
    val f = MongoSource.findOneAndDelete(col, filter).runToFuture(s)

    //then
    s.tick(1.second)
    verify(col).findOneAndDelete(filter)
    f.value.get shouldBe util.Success(Some(employee))
  }

  it should "find one and delete with options" in {
    //given
    val retryStrategy = RetryStrategy(attempts = 3)
    val s = TestScheduler()
    val filter = Filters.and(Filters.eq("city", "Cracow"), Filters.gt("age", 66))
    val employee = genEmployee.sample.get
    val failedPub = Task.raiseError[Employee](DummyException("Find one and delete, failed")).toReactivePublisher(s)
    val successPub = Task(employee).toReactivePublisher(s)
    when(col.findOneAndDelete(filter, DefaultFindOneAndDeleteOptions))
      .thenReturn(failedPub, successPub)

    //when
    val f = MongoSource
      .findOneAndDelete(col, filter, findOneAndDeleteOptions = DefaultFindOneAndDeleteOptions, retryStrategy)
      .runToFuture(s)

    //then
    s.tick(1.second)
    verify(col, times(2)).findOneAndDelete(filter, DefaultFindOneAndDeleteOptions)
    f.value.get shouldBe util.Success(Some(employee))
  }

  it should "return false whenever the underlying publisher did not emitted any elements" in {
    //given
    val s = TestScheduler()
    val filter = Filters.eq("name", "X")
    val publisher = Observable.empty[Employee].toReactivePublisher(s)
    when(col.findOneAndDelete(filter)).thenReturn(publisher)

    //when
    val f = MongoSource.findOneAndDelete(col, filter).runToFuture(s)

    //then
    s.tick(1.second)
    verify(col).findOneAndDelete(filter)
    f.value.get shouldBe util.Success(Option.empty[Employee])
  }

  it should "return a failed task whenever the underlying publisher signaled error" in {
    //given
    val s = TestScheduler()
    val filter = Filters.eq("name", "X")
    val ex = DummyException("Find one and delete, failed")
    val publisher = Task.raiseError[Employee](ex).toReactivePublisher(s)
    when(col.findOneAndDelete(filter)).thenReturn(publisher)

    //when
    val f = MongoSource.findOneAndDelete(col, filter).runToFuture(s)

    //then
    s.tick(1.second)
    verify(col).findOneAndDelete(filter)
    f.value.get shouldBe util.Failure(ex)
  }

  "findOneAndReplace" should "return the same element that was replaced" in {
    //given
    val s = TestScheduler()
    val filter = Filters.and(Filters.eq("city", "Cadiz"), Filters.eq("name", "Gustavo"))
    val e = Employee(name = "Gustavo", city = "Cadiz", age = 45)
    val replacement = Employee(name = "Alberto", city = "Cadiz", age = 33)
    val publisher = Task(e).toReactivePublisher(s)
    when(col.findOneAndReplace(filter, replacement)).thenReturn(publisher)

    //when
    val f = MongoSource.findOneAndReplace(col, filter, replacement).runToFuture(s)

    //then
    s.tick(1.second)
    verify(col).findOneAndReplace(filter, replacement)
    f.value.get shouldBe util.Success(Some(e))
  }

  it should "find one and replace with options" in {
    //given
    val retryStrategy = RetryStrategy(attempts = 1)
    val s = TestScheduler()
    val filter = Filters.eq("city", "Cape Town")
    val e = genEmployee.sample.get
    val replacement = genEmployee.sample.get

    //and
    val failedPub = Task.raiseError[Employee](DummyException("Find one and replace, failed")).toReactivePublisher(s)
    val successPub = Task(e).toReactivePublisher(s)

    //when
    when(col.findOneAndReplace(filter, replacement, DefaultFindOneAndReplaceOptions))
      .thenReturn(failedPub, successPub)

    //and
    val f = MongoSource
      .findOneAndReplace(
        col,
        filter,
        replacement,
        findOneAndReplaceOptions = DefaultFindOneAndReplaceOptions,
        retryStrategy)
      .runToFuture(s)

    //then
    s.tick(1.second)
    verify(col, times(2)).findOneAndReplace(filter, replacement, DefaultFindOneAndReplaceOptions)
    f.value.get shouldBe util.Success(Some(e))
  }

  it should "return false whenever the underlying publisher did not emitted any elements" in {
    //given
    val s = TestScheduler()
    val filter = Filters.eq("name", "X")
    val replacement = genEmployee.sample.get
    val publisher = Observable.empty[Employee].toReactivePublisher(s)
    when(col.findOneAndReplace(filter, replacement)).thenReturn(publisher)

    //when
    val f = MongoSource.findOneAndReplace(col, filter, replacement).runToFuture(s)

    //then
    s.tick(1.second)
    verify(col).findOneAndReplace(filter, replacement)
    f.value.get shouldBe util.Success(Option.empty[Employee])
  }

  it should "return a failed task whenever the underlying publisher signaled error" in {
    //given
    val s = TestScheduler()
    val filter = Filters.eq("name", "X")
    val replacement = genEmployee.sample.get
    val ex = DummyException("Find one and replace, failed")
    val publisher = Task.raiseError[Employee](ex).toReactivePublisher(s)
    when(col.findOneAndReplace(filter, replacement)).thenReturn(publisher)

    //when
    val f = MongoSource.findOneAndReplace(col, filter, replacement).runToFuture(s)

    //then
    s.tick(1.second)
    verify(col).findOneAndReplace(filter, replacement)
    f.value.get shouldBe util.Failure(ex)
  }

  "findOneAndUpdate" should "return the same element that was updated" in {
    //given
    val s = TestScheduler()
    val filter = Filters.eq("name", "Salomon")
    val e = genEmployee.sample.get
    val update = Updates.inc("age", 1)
    val publisher = Task(e).toReactivePublisher(s)
    when(col.findOneAndUpdate(filter, update)).thenReturn(publisher)

    //when
    val f = MongoSource.findOneAndUpdate(col, filter, update).runToFuture(s)

    //then
    s.tick(1.second)
    verify(col).findOneAndUpdate(filter, update)
    f.value.get shouldBe util.Success(Some(e))
  }

  it should "find one and update with options" in {
    //given
    val retryStrategy = RetryStrategy(attempts = 1)
    val s = TestScheduler()
    val filter = Filters.eq("city", "Moscou")
    val e = genEmployee.sample.get
    val update = Updates.inc("age", 1)

    //and
    val failedPub = Task.raiseError[Employee](DummyException("Find one and update, failed")).toReactivePublisher(s)
    val successPub = Task(e).toReactivePublisher(s)

    //when
    when(col.findOneAndUpdate(filter, update, DefaultFindOneAndUpdateOptions))
      .thenReturn(failedPub, successPub)

    //and
    val f = MongoSource
      .findOneAndUpdate(col, filter, update, findOneAndUpdateOptions = DefaultFindOneAndUpdateOptions, retryStrategy)
      .runToFuture(s)

    //then
    s.tick(1.second)
    verify(col, times(2)).findOneAndUpdate(filter, update, DefaultFindOneAndUpdateOptions)
    f.value.get shouldBe util.Success(Some(e))
  }

  it should "return false whenever the underlying publisher did not emitted any elements" in {
    //given
    val s = TestScheduler()
    val filter = Filters.eq("name", "X")
    val update = Updates.inc("age", 1)
    val publisher = Observable.empty[Employee].toReactivePublisher(s)
    when(col.findOneAndUpdate(filter, update)).thenReturn(publisher)

    //when
    val f = MongoSource.findOneAndUpdate(col, filter, update).runToFuture(s)

    //then
    s.tick(1.second)
    verify(col).findOneAndUpdate(filter, update)
    f.value.get shouldBe util.Success(Option.empty[Employee])
  }

  it should "return a failed task whenever the underlying publisher signaled error" in {
    //given
    val s = TestScheduler()
    val filter = Filters.eq("name", "X")
    val update = Updates.inc("age", 1)
    val ex = DummyException("Find one and update, failed")
    val publisher = Task.raiseError[Employee](ex).toReactivePublisher(s)
    when(col.findOneAndUpdate(filter, update)).thenReturn(publisher)

    //when
    val f = MongoSource.findOneAndUpdate(col, filter, update).runToFuture(s)

    //then
    s.tick(1.second)
    verify(col).findOneAndUpdate(filter, update)
    f.value.get shouldBe util.Failure(ex)
  }

}
