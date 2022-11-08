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

import com.mongodb.reactivestreams.client.MongoCollection
import org.scalatest.flatspec.AnyFlatSpecLike
import com.mongodb.client.model.{Filters, Updates}
import com.mongodb.client.result.{
  DeleteResult => MongoDeleteResult,
  InsertManyResult => MongoInsertManyResult,
  InsertOneResult => MongoInsertOneResult,
  UpdateResult => MongoUpdateResult
}
import monix.connect.mongodb.domain.{
  DefaultDeleteOptions,
  DefaultDeleteResult,
  DefaultInsertManyOptions,
  DefaultInsertOneOptions,
  DefaultReplaceOptions,
  DefaultUpdateOptions,
  InsertManyResult,
  InsertOneResult,
  UpdateResult
}
import monix.eval.Task
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.mockito.MockitoSugar.{times, verify, when}
import monix.execution.exceptions.DummyException
import monix.reactive.Observable
import org.reactivestreams.Publisher
import monix.execution.Scheduler.Implicits.global

import scala.util.Failure
import monix.execution.schedulers.TestScheduler
import org.bson.BsonValue
import org.mockito.IdiomaticMockito
import org.mongodb.scala.bson.BsonObjectId
import org.scalacheck.Gen
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

@deprecated("0.5.3")
class MongoOpSpec
  extends AnyFlatSpecLike with TestFixture with ScalaFutures with Matchers with BeforeAndAfterEach
  with IdiomaticMockito {

  private[this] implicit val col: MongoCollection[Employee] = mock[MongoCollection[Employee]]
  private[this] implicit val defaultConfig: PatienceConfig = PatienceConfig(5.seconds, 300.milliseconds)
  private[this] val objectId = BsonObjectId.apply()

  override def beforeEach() = {
    reset(col)
    super.beforeEach()
  }

  "deleteOne" should "delete one single element" in {
    //given
    val filter = Filters.gt("age", 50)
    val publisher: Publisher[MongoDeleteResult] = Task(MongoDeleteResult.acknowledged(1L)).toReactivePublisher(global)
    when(col.deleteOne(filter)).thenReturn(publisher)

    //when
    val r = MongoOp.deleteOne(col, filter).runSyncUnsafe()

    //then
    verify(col).deleteOne(filter)
    r.deleteCount shouldBe 1L
    r.wasAcknowledged shouldBe true
  }

  it should "delete one single element with options" in {
    //given
    val s = TestScheduler()
    val filter = Filters.gt("age", 50)
    val failedPub = Task.raiseError[MongoDeleteResult](DummyException("Delete one failed")).toReactivePublisher(s)
    val delayedPub = Task(MongoDeleteResult.acknowledged(0L)).delayResult(500.millis).toReactivePublisher(s)
    val publisher = Task(MongoDeleteResult.acknowledged(1L)).toReactivePublisher(s)
    when(col.deleteOne(filter, DefaultDeleteOptions)).thenReturn(delayedPub, failedPub, publisher)

    //when
    val f = MongoOp
      .deleteOne(col, filter, deleteOptions = DefaultDeleteOptions, retries = 3, timeout = Some(150.millis))
      .runToFuture(s)

    //then
    s.tick(1.second)
    whenReady(f) { r =>
      verify(col, times(3)).deleteOne(filter, DefaultDeleteOptions)
      r.deleteCount shouldBe 1L
      r.wasAcknowledged shouldBe true
    }
  }

  it should "return an empty option of delete result when the publisher did not emitted any element" in {
    //given
    val filter = Filters.gt("age", 50)
    val publisher: Publisher[MongoDeleteResult] = Observable.empty[MongoDeleteResult].toReactivePublisher(global)
    when(col.deleteOne(filter)).thenReturn(publisher)

    //when
    val r = MongoOp.deleteOne(col, filter).runSyncUnsafe()

    //then
    verify(col).deleteOne(filter)
    r shouldBe DefaultDeleteResult
  }

  it should "return a failed task when underlying publisher signaled error" in {
    //given
    val s = TestScheduler()
    val exception = DummyException("Mongodb internal server error.")
    val publisher: Publisher[MongoDeleteResult] = Task.raiseError[MongoDeleteResult](exception).toReactivePublisher(s)
    val filter = Filters.gt("age", 50)
    when(col.deleteOne(filter)).thenReturn(publisher)

    //when
    val f = MongoOp.deleteOne(col, filter).runToFuture(s)
    s.tick(500.milliseconds)

    //then
    verify(col).deleteOne(filter)
    f.value shouldBe Some(Failure(exception))
  }

  "deleteMany" should "delete many elements without delete options" in {
    //given
    val filter = Filters.gt("age", 50)
    val publisher: Publisher[MongoDeleteResult] =
      Task(MongoDeleteResult.acknowledged(1000L)).toReactivePublisher(global)
    when(col.deleteMany(filter)).thenReturn(publisher)

    //when
    val r = MongoOp.deleteMany(col, filter).runSyncUnsafe()

    //then
    verify(col).deleteMany(filter)
    r.deleteCount shouldBe 1000L
    r.wasAcknowledged shouldBe true
  }

  it should "delete many elements with options" in {
    //given
    val s = TestScheduler()
    val filter = Filters.gt("age", 50)
    val failedPub = Task.raiseError[MongoDeleteResult](DummyException("Delete many failed")).toReactivePublisher(s)
    val delayedPub = Task(MongoDeleteResult.acknowledged(10L)).delayResult(500.millis).toReactivePublisher(s)
    val publisher: Publisher[MongoDeleteResult] = Task(MongoDeleteResult.acknowledged(1000L)).toReactivePublisher(s)
    when(col.deleteMany(filter, DefaultDeleteOptions)).thenReturn(delayedPub, failedPub, publisher)

    //when
    val f =
      MongoOp.deleteMany(col, filter, DefaultDeleteOptions, retries = 3, timeout = Some(150.millis)).runToFuture(s)

    //then
    s.tick(1.second)
    whenReady(f) { r =>
      verify(col, times(3)).deleteMany(filter, DefaultDeleteOptions)
      r.deleteCount shouldBe 1000L
      r.wasAcknowledged shouldBe true
    }
  }

  it should " return an empty option when the publisher did not emitted any elements" in {
    //given
    val filter = Filters.gt("age", 50)
    val publisher: Publisher[MongoDeleteResult] = Observable.empty[MongoDeleteResult].toReactivePublisher(global)
    when(col.deleteMany(filter)).thenReturn(publisher)

    //when
    val r = MongoOp.deleteMany(col, filter).runSyncUnsafe()

    //then
    verify(col).deleteMany(filter)
    r shouldBe DefaultDeleteResult
  }

  it should "return an failed task when underlying publisher signaled error" in {
    //given
    val s = TestScheduler()
    val exception = DummyException("Mongodb internal server error.")
    val publisher: Publisher[MongoDeleteResult] = Task.raiseError[MongoDeleteResult](exception).toReactivePublisher(s)
    val filter = Filters.gt("age", 50)
    when(col.deleteMany(filter)).thenReturn(publisher)

    //when
    val f = MongoOp.deleteMany(col, filter).runToFuture(s)
    s.tick(500.milliseconds)

    //then
    verify(col).deleteMany(filter)
    f.value shouldBe Some(Failure(exception))
  }

  "insertOne" should "insert one single element" in {
    //given
    val e = genEmployee.sample.get
    val insertOneResult = MongoInsertOneResult.acknowledged(objectId)
    val publisher: Publisher[MongoInsertOneResult] = Task(insertOneResult).toReactivePublisher(global)
    when(col.insertOne(e)).thenReturn(publisher)

    //when
    val r = MongoOp.insertOne(col, e).runSyncUnsafe()

    //then
    verify(col).insertOne(e)
    r shouldBe InsertOneResult(Some(objectId.getValue.toString), true)
  }

  it should "insert one single element with options" in {
    //given
    val s = TestScheduler()
    val employee = genEmployee.sample.get
    val insertOneResult = MongoInsertOneResult.acknowledged(objectId)
    val delayedPub = Task(insertOneResult).delayResult(500.millis).toReactivePublisher(s)
    val failedPub = Task.raiseError[MongoInsertOneResult](DummyException("Insert one failed")).toReactivePublisher(s)
    val publisher: Publisher[MongoInsertOneResult] =
      Task(MongoInsertOneResult.acknowledged(objectId)).toReactivePublisher(s)
    when(col.insertOne(employee, DefaultInsertOneOptions)).thenReturn(delayedPub, failedPub, publisher)

    //when
    val f = MongoOp
      .insertOne(col, employee, insertOneOptions = DefaultInsertOneOptions, retries = 3, timeout = Some(150.millis))
      .runToFuture(s)

    //then
    s.tick(1.second)
    whenReady(f) { r =>
      verify(col, times(3)).insertOne(employee, DefaultInsertOneOptions)
      r shouldBe InsertOneResult(Some(objectId.getValue.toString), true)
    }
  }

  it should "return false whenever the underlying publisher did not emitted any elements" in {
    //given
    val e = genEmployee.sample.get
    when(col.insertOne(e)).thenReturn(Observable.empty.toReactivePublisher(global))

    //when
    val f = MongoOp.insertOne(col, e).runToFuture(global)

    //then
    verify(col).insertOne(e)
    f.value.get shouldBe util.Success(InsertOneResult(None, false))
  }

  it should "return a failed task whenever the underlying publisher signaled error" in {
    //given
    val s = TestScheduler()
    val exception = DummyException("Insert one failed")
    val e = genEmployee.sample.get
    val publisher = Task.raiseError[MongoInsertOneResult](exception).toReactivePublisher(s)
    when(col.insertOne(e)).thenReturn(publisher)

    //when
    val f = MongoOp.insertOne(col, e).runToFuture(s)

    //then
    s.tick(1.second)
    verify(col).insertOne(e)
    f.value shouldBe Some(Failure(exception))
  }

  "insertMany" should "insert many elements" in {
    //given
    val s = TestScheduler()
    val l = Gen.listOfN(10, genEmployee).sample.get
    val publisher: Publisher[MongoInsertManyResult] =
      Task(MongoInsertManyResult.acknowledged(Map.empty[Integer, BsonValue].asJava)).toReactivePublisher(s)
    when(col.insertMany(l.asJava)).thenReturn(publisher)

    //when
    val f = MongoOp.insertMany(col, l).runToFuture(s)

    //then
    s.tick(1.second)
    verify(col).insertMany(l.asJava)
    f.value.get shouldBe util.Success(InsertManyResult(Set(), true))
  }

  it should "insert many elements with options" in {
    //given
    val s = TestScheduler()
    val employees = Gen.listOfN(10, genEmployee).sample.get
    val delayedPub = Task(MongoInsertManyResult.acknowledged(Map.empty[Integer, BsonValue].asJava))
      .delayResult(500.millis)
      .toReactivePublisher(s)
    val failedPub = Task.raiseError[MongoInsertManyResult](DummyException("Insert one failed")).toReactivePublisher(s)
    val successPub: Publisher[MongoInsertManyResult] =
      Task(MongoInsertManyResult.acknowledged(Map.empty[Integer, BsonValue].asJava)).toReactivePublisher(s)
    when(col.insertMany(employees.asJava, DefaultInsertManyOptions)).thenReturn(delayedPub, failedPub, successPub)

    //when
    val f = MongoOp
      .insertMany(col, employees, insertManyOptions = DefaultInsertManyOptions, retries = 3, timeout = Some(150.millis))
      .runToFuture(s)

    //then
    s.tick(1.second)
    verify(col, times(3)).insertMany(employees.asJava, DefaultInsertManyOptions)
    f.value.get shouldBe util.Success(InsertManyResult(Set(), true))
  }

  it should "return false whenever the underlying publisher did not emitted any elements" in {
    //given
    val l = Gen.listOfN(10, genEmployee).sample.get
    when(col.insertMany(l.asJava)).thenReturn(Observable.empty.toReactivePublisher(global))

    //when
    val f = MongoOp.insertMany(col, l).runToFuture(global)

    //then
    verify(col).insertMany(l.asJava)
    f.value.get shouldBe util.Success(InsertManyResult(Set(), false))
  }

  it should "return a failed task whenever the underlying publisher signaled error" in {
    //given
    val s = TestScheduler()
    val exception = DummyException("Insert many failed")
    val l = Gen.listOfN(10, genEmployee).sample.get
    val publisher = Task.raiseError[MongoInsertManyResult](exception).toReactivePublisher(s)
    when(col.insertMany(l.asJava)).thenReturn(publisher)

    //when
    val f = MongoOp.insertMany(col, l).runToFuture(s)

    //then
    s.tick(1.second)
    verify(col).insertMany(l.asJava)
    f.value shouldBe Some(Failure(exception))
  }

  "updateOne" should "update one single element" in {
    //given
    val s = TestScheduler()
    val filter = Filters.eq("city", "Girona")
    val update = Updates.set("city", "Milton Keynes")
    val updateResult = MongoUpdateResult.acknowledged(10L, 1L, null)
    val publisher: Publisher[MongoUpdateResult] = Task(updateResult).toReactivePublisher(s)
    when(col.updateOne(filter, update)).thenReturn(publisher)

    //when
    val f = MongoOp.updateOne(col, filter, update).runToFuture(s)

    //then
    s.tick(1.second)
    verify(col).updateOne(filter, update)
    f.value.get shouldBe util.Success(UpdateResult(10L, 1, true))
  }

  it should "update one single element with options" in {
    //given
    val s = TestScheduler()
    val filter = Filters.eq("city", "Liverpool")
    val update = Updates.set("city", "Geneva")
    val updateResult = MongoUpdateResult.acknowledged(1L, 1L, null)
    val delayedPub = Task(updateResult).delayResult(500.millis).toReactivePublisher(s)
    val failedPub = Task.raiseError[MongoUpdateResult](DummyException("Insert one failed")).toReactivePublisher(s)
    val successPub: Publisher[MongoUpdateResult] = Task(updateResult).toReactivePublisher(s)
    when(col.updateOne(filter, update, DefaultUpdateOptions)).thenReturn(delayedPub, failedPub, successPub)

    //when
    val f = MongoOp
      .updateOne(col, filter, update, updateOptions = DefaultUpdateOptions, retries = 3, timeout = Some(150.millis))
      .runToFuture(s)

    //then
    s.tick(1.second)
    verify(col, times(3)).updateOne(filter, update, DefaultUpdateOptions)
    f.value.get shouldBe util.Success(UpdateResult(1L, 1L, true))
  }

  it should "return empty whenever the underlying publisher did not emitted any elements" in {
    //given
    val s = TestScheduler()
    val filter = Filters.eq("name", "a")
    val update = Updates.set("name", "b")
    val publisher: Publisher[MongoUpdateResult] = Observable.empty[MongoUpdateResult].toReactivePublisher(s)
    when(col.updateOne(filter, update)).thenReturn(publisher)

    //when
    val f = MongoOp.updateOne(col, filter, update).runToFuture(s)

    //then
    s.tick(1.second)
    verify(col).updateOne(filter, update)
    val expectedUpdateResult = UpdateResult(matchedCount = 0, modifiedCount = 0, wasAcknowledged = false)
    f.value.get shouldBe util.Success(UpdateResult(0, 0, false))
  }

  it should "return a failed task whenever the underlying publisher signaled error" in {
    //given
    val s = TestScheduler()
    val filter = Filters.eq("name", "a")
    val update = Updates.set("name", "b")
    val ex = DummyException("Insert many failed")
    val publisher: Publisher[MongoUpdateResult] = Task.raiseError[MongoUpdateResult](ex).toReactivePublisher(s)
    when(col.updateOne(filter, update)).thenReturn(publisher)

    //when
    val f = MongoOp.updateOne(col, filter, update).runToFuture(s)

    //then
    s.tick(1.second)
    verify(col).updateOne(filter, update)
    f.value.get shouldBe util.Failure(ex)
  }

  "updateMany" should "update many elements" in {
    //given
    val s = TestScheduler()
    val filter = Filters.eq("city", "Tokyo")
    val update = Updates.set("city", "Madrid")
    val updateResult = MongoUpdateResult.acknowledged(10L, 10L, null)
    val publisher: Publisher[MongoUpdateResult] = Task(updateResult).toReactivePublisher(s)
    when(col.updateMany(filter, update)).thenReturn(publisher)

    //when
    val f = MongoOp.updateMany(col, filter, update).runToFuture(s)

    //then
    s.tick(1.second)
    verify(col).updateMany(filter, update)
    f.value.get shouldBe util.Success(UpdateResult(10L, 10L, true))
  }

  it should "update many elements with options" in {
    //given
    val s = TestScheduler()
    val filter = Filters.eq("name", "A")
    val update = Updates.set("city", "B")
    val updateResult = MongoUpdateResult.acknowledged(10L, 10L, null)
    val delayedPub = Task(updateResult).delayResult(500.millis).toReactivePublisher(s)
    val failedPub = Task.raiseError[MongoUpdateResult](DummyException("Insert one failed")).toReactivePublisher(s)
    val successPub: Publisher[MongoUpdateResult] = Task(updateResult).toReactivePublisher(s)
    when(col.updateMany(filter, update, DefaultUpdateOptions)).thenReturn(delayedPub, failedPub, successPub)

    //when
    val f = MongoOp
      .updateMany(col, filter, update, updateOptions = DefaultUpdateOptions, retries = 3, timeout = Some(150.millis))
      .runToFuture(s)

    //then
    s.tick(1.second)
    verify(col, times(3)).updateMany(filter, update, DefaultUpdateOptions)
    f.value.get shouldBe util.Success(UpdateResult(10L, 10L, true))
  }

  it should "return false whenever the underlying publisher did not emitted any elements" in {
    //given
    val s = TestScheduler()
    val filter = Filters.eq("name", "A")
    val update = Updates.set("name", "B")
    val publisher: Publisher[MongoUpdateResult] = Observable.empty[MongoUpdateResult].toReactivePublisher(s)
    when(col.updateMany(filter, update)).thenReturn(publisher)

    //when
    val f = MongoOp.updateMany(col, filter, update).runToFuture(s)

    //then
    s.tick(1.second)
    verify(col).updateMany(filter, update)
    f.value.get shouldBe util.Success(UpdateResult(0L, 0L, false))
  }

  it should "return a failed task whenever the underlying publisher signaled error" in {
    //given
    val s = TestScheduler()
    val filter = Filters.eq("name", "A")
    val update = Updates.set("name", "B")
    val ex = DummyException("Insert many failed")
    val publisher: Publisher[MongoUpdateResult] = Task.raiseError[MongoUpdateResult](ex).toReactivePublisher(s)
    when(col.updateMany(filter, update)).thenReturn(publisher)

    //when
    val f = MongoOp.updateMany(col, filter, update).runToFuture(s)

    //then
    s.tick(1.second)
    verify(col).updateMany(filter, update)
    f.value.get shouldBe util.Failure(ex)
  }

  "replaceOne" should "update one single element" in {
    //given
    val s = TestScheduler()
    val filter = Filters.eq("name", "Alex")
    val employee = genEmployee.sample.get
    val updateResult = MongoUpdateResult.acknowledged(10L, 1L, null)
    val publisher = Task(updateResult).toReactivePublisher(s)
    when(col.replaceOne(filter, employee)).thenReturn(publisher)

    //when
    val f = MongoOp.replaceOne(col, filter, employee).runToFuture(s)

    //then
    s.tick(1.second)
    verify(col).replaceOne(filter, employee)
    f.value.get shouldBe util.Success(UpdateResult(10L, 1L, true))
  }

  it should "replace one single element with options" in {
    //given
    val s = TestScheduler()
    val filter = Filters.eq("city", "Bucarest")
    val employee = genEmployee.sample.get
    val updateResult = MongoUpdateResult.acknowledged(1L, 1L, null)
    val delayedPub = Task(updateResult).delayResult(500.millis).toReactivePublisher(s)
    val failedPub = Task.raiseError[MongoUpdateResult](DummyException("Replace one failed")).toReactivePublisher(s)
    val successPub: Publisher[MongoUpdateResult] = Task(updateResult).toReactivePublisher(s)
    when(col.replaceOne(filter, employee, DefaultReplaceOptions))
      .thenReturn(delayedPub, failedPub, successPub)

    //when
    val f = MongoOp
      .replaceOne(
        col,
        filter,
        employee,
        replaceOptions = DefaultReplaceOptions,
        retries = 3,
        timeout = Some(150.millis))
      .runToFuture(s)

    //then
    s.tick(1.second)
    verify(col, times(3)).replaceOne(filter, employee, DefaultReplaceOptions)
    f.value.get shouldBe util.Success(UpdateResult(1L, 1L, true))
  }

  it should "return false whenever the underlying publisher did not emitted any elements" in {
    //given
    val s = TestScheduler()
    val filter = Filters.eq("name", "a")
    val e = genEmployee.sample.get
    val publisher: Publisher[MongoUpdateResult] = Observable.empty[MongoUpdateResult].toReactivePublisher(s)
    when(col.replaceOne(filter, e)).thenReturn(publisher)

    //when
    val f = MongoOp.replaceOne(col, filter, e).runToFuture(s)

    //then
    s.tick(1.second)
    verify(col).replaceOne(filter, e)
    f.value.get shouldBe util.Success(UpdateResult(0L, 0L, false))
  }

  it should "return a failed task whenever the underlying publisher signaled error" in {
    //given
    val s = TestScheduler()
    val filter = Filters.eq("name", "X")
    val e = genEmployee.sample.get
    val ex = DummyException("Replace one failed")
    val publisher: Publisher[MongoUpdateResult] = Task.raiseError[MongoUpdateResult](ex).toReactivePublisher(s)
    when(col.replaceOne(filter, e)).thenReturn(publisher)

    //when
    val f = MongoOp.replaceOne(col, filter, e).runToFuture(s)

    //then
    s.tick(1.second)
    verify(col).replaceOne(filter, e)
    f.value.get shouldBe util.Failure(ex)
  }

}
