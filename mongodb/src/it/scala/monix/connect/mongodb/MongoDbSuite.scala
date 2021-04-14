/*
 * Copyright (c) 2020-2021 by The Monix Connect Project Developers.
 * See the project homepage at: https://monix.io
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

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class MongoDbSuite extends AnyFlatSpecLike with Fixture with Matchers with BeforeAndAfterEach {

  override def beforeEach() = {
    super.beforeEach()
    MongoDb.dropDatabase(db).runSyncUnsafe()
    MongoDb.dropCollection(db, employeesColName).runSyncUnsafe()
  }

  s"$MongoDb" should "list database names" in {
    //given
    val dbNames = Gen.listOfN(5, genNonEmptyStr).sample.get
    val existedBefore = MongoDb.listDatabases(client).filter(dbNames.contains(_)).toListL.runSyncUnsafe().nonEmpty
    Task.sequence(dbNames.map(name => MongoDb.createCollection(client.getDatabase(name), name))).runSyncUnsafe()

    //when
    val dbs = MongoDb.listDatabases(client).toListL.runSyncUnsafe()

    //then
    existedBefore shouldBe false
    dbs.isEmpty shouldBe false
    dbs.count(dbNames.contains(_)) shouldBe dbNames.size
  }

  it should "create a collection" in {
    //given
    val collectionName = genNonEmptyStr.sample.get
    val existedBefore = MongoDb.existsCollection(db, collectionName).runSyncUnsafe()

    //when
    val r = MongoDb.createCollection(db, collectionName).runSyncUnsafe()

    //then
    val exists = MongoDb.existsCollection(db, collectionName).runSyncUnsafe()
    r shouldBe a[Unit]
    existedBefore shouldBe false
    exists shouldBe true
  }

  it should "drop a db" in {
    //given
    val dbName = genNonEmptyStr.sample.get
    val database = client.getDatabase(dbName)
    MongoDb.createCollection(database, genNonEmptyStr.sample.get).runSyncUnsafe() //triggers db creation
    val existedBefore = MongoDb.existsDatabase(client, dbName).runSyncUnsafe()

    //when
    val r = MongoDb.dropDatabase(db).runSyncUnsafe()

    //then
    val exists = MongoDb.existsCollection(db, employeesColName).runSyncUnsafe()
    r shouldBe a[Unit]
    existedBefore shouldBe true
    exists shouldBe false
  }

  it should "drop a collection" in {
    //given
    val dbName = genNonEmptyStr.sample.get
    val database = client.getDatabase(dbName)
    val collection = genNonEmptyStr.sample.get
    MongoDb.createCollection(database, collection).runSyncUnsafe()
    val existedBefore = MongoDb.existsCollection(database, collection).runSyncUnsafe()

    //when
    val r = MongoDb.dropCollection(db, collection).runSyncUnsafe()

    //then
    val exists = MongoDb.existsCollection(db, employeesColName).runSyncUnsafe()
    r shouldBe a[Unit]
    existedBefore shouldBe true
    exists shouldBe false
  }

  it should "check if a collection exists" in {
    //given
    val collectionName = genNonEmptyStr.sample.get

    //when
    val existedBefore = MongoDb.existsCollection(db, collectionName).runSyncUnsafe()

    //and
    val r = MongoDb.createCollection(db, collectionName).runSyncUnsafe()

    //and
    val existedAfter = MongoDb.existsCollection(db, collectionName).runSyncUnsafe()

    //then
    r shouldBe a[Unit]
    existedBefore shouldBe false
    existedAfter shouldBe true
  }

  it should "check if a database exists" in {
    //given
    val dbName = genNonEmptyStr.sample.get
    val database = client.getDatabase(dbName)

    //when
    val existedBefore = MongoDb.existsDatabase(client, dbName).runSyncUnsafe()

    //and
    val r = MongoDb.createCollection(database, "myCollection2").runSyncUnsafe()

    //and
    val existedAfter = MongoDb.existsDatabase(client, dbName).runSyncUnsafe()

    //then
    r shouldBe a[Unit]
    existedBefore shouldBe false
    existedAfter shouldBe true
  }

  it should "list collections" in {
    //given
    val collectionNames: Seq[String] = Gen.listOfN(10, Gen.identifier.map("test-" + _.take(20))).sample.get
    Task.sequence(collectionNames.map(MongoDb.createCollection(db, _))).runSyncUnsafe()

    //when
    val l = MongoDb.listCollections(db).toListL.runSyncUnsafe()

    //then
    l should not be empty
    l should contain theSameElementsAs collectionNames
  }

}
