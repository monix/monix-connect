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

import monix.connect.mongodb.client.{CollectionDocumentRef, MongoConnection}
import monix.eval.Task
import monix.execution.Scheduler
import monix.testing.scalatest.MonixTaskSpec
import org.scalacheck.Gen
import org.scalatest.{Assertion, BeforeAndAfterEach}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

class MongoDbSuite extends AsyncFlatSpec with MonixTaskSpec with Fixture with Matchers with BeforeAndAfterEach {

  override implicit val scheduler: Scheduler = Scheduler.io("mongo-db-suite")

  override def beforeEach() = {
    super.beforeEach()
    MongoDb.dropDatabase(db).runSyncUnsafe()
    MongoDb.dropCollection(db, employeesColName).runSyncUnsafe()
  }

  s"$MongoDb" should "create a collection within the current db" in {
    val dbName = genNonEmptyStr.sample.get
    val newCollection = genNonEmptyStr.sample.get
    val collectionRef = CollectionDocumentRef(dbName, "randomColName")

    MongoConnection.create1(mongoEndpoint, collectionRef).use[Task, Assertion]{ operator =>
      for {
        existedBefore <- operator.db.existsCollection(newCollection)
        _ <- operator.db.createCollection(newCollection)
        existsAfter <- operator.db.existsCollection(newCollection)
      } yield {
        existedBefore shouldBe false
        existsAfter shouldBe true
      }
    }
  }

  it should "create a collection outside the current db" in {
    val externalDbName = genNonEmptyStr.sample.get
    val newCollection = genNonEmptyStr.sample.get
    val collectionRef = CollectionDocumentRef("randomDb", "randomColName")

    MongoConnection.create1(mongoEndpoint, collectionRef).use[Task, Assertion]{ operator =>
      for {
        existedBefore <- operator.db.existsCollection(externalDbName, newCollection)
        _ <- operator.db.createCollection(externalDbName, newCollection)
        existsAfter <- operator.db.existsCollection(externalDbName, newCollection)
      } yield {
        existedBefore shouldBe false
        existsAfter shouldBe true
      }
    }
  }

  it should "drop the current db" in {
    val dbName = genNonEmptyStr.sample.get
    val collection = genNonEmptyStr.sample.get
    val collectionRef = CollectionDocumentRef(dbName, collection)

    MongoConnection.create1(mongoEndpoint, collectionRef).use[Task, Assertion]{ operator =>
      for {
        existedBefore <- operator.db.existsDatabase(dbName)
        _ <- operator.db.dropDatabase
        existsAfter <- operator.db.existsDatabase(dbName)
      } yield {
        existedBefore shouldBe true
        existsAfter shouldBe false
      }
    }
  }

  it should "drop a collection from the current db" in {
    val dbName = genNonEmptyStr.sample.get
    val collection = genNonEmptyStr.sample.get
    val collectionRef = CollectionDocumentRef(dbName, "randomColName")

    MongoConnection.create1(mongoEndpoint, collectionRef).use[Task, Assertion] { operator =>
      for {
        _ <- operator.db.createCollection(collection)
        existedBefore <- operator.db.existsCollection(collection)
        _ <- operator.db.dropCollection(collection)
        existsAfter <- operator.db.existsCollection(collection)
      } yield {
        existedBefore shouldBe true
        existsAfter shouldBe false
      }
    }
  }

  it should "drop a collection from outside the current db" in {
    val otherDbName = genNonEmptyStr.sample.get
    val collection = genNonEmptyStr.sample.get
    val collectionRef = CollectionDocumentRef("randomDb", "randomColName")

    MongoConnection.create1(mongoEndpoint, collectionRef).use[Task, Assertion] { operator =>
      for {
        _ <- operator.db.createCollection(otherDbName, collection)
        existedBefore <- operator.db.existsCollection(otherDbName, collection)
        _ <- operator.db.dropCollection(otherDbName, collection)
        existsAfter <- operator.db.existsCollection(otherDbName, collection)
      } yield {
        existedBefore shouldBe true
        existsAfter shouldBe false
      }
    }
  }

  it should "check if a collection exists in the current db" in {
    val collectionName = genNonEmptyStr.sample.get
    val collectionRef = CollectionDocumentRef(dbName, "randomColName")

    MongoConnection.create1(mongoEndpoint, collectionRef).use[Task, Assertion]{ operator =>
      for {
        existedBefore <- operator.db.existsCollection(collectionName)
        _ <- operator.db.createCollection(collectionName)
        existsAfter <- operator.db.existsCollection(collectionName)
      } yield {
        existedBefore shouldBe false
        existsAfter shouldBe true
      }
    }
  }

  it should "check if a collection in exists outside the current db" in {
    val dbName = genNonEmptyStr.sample.get
    val collectionName = genNonEmptyStr.sample.get
    val collectionRef = CollectionDocumentRef(dbName, "randomColName")

    MongoConnection.create1(mongoEndpoint, collectionRef).use[Task, Assertion]{ operator =>
      for {
        existedBefore <- operator.db.existsCollection(dbName, collectionName)
        _ <- operator.db.createCollection(dbName, collectionName)
        existsAfter <- operator.db.existsCollection(dbName, collectionName)
      } yield {
        existedBefore shouldBe false
        existsAfter shouldBe true
      }
    }
  }

  it should "check if a database exists" in {
    val dbName = genNonEmptyStr.sample.get
    val collectionName = genNonEmptyStr.sample.get
    val collectionRef = CollectionDocumentRef("randomDb", "randomColName")

    MongoConnection.create1(mongoEndpoint, collectionRef).use[Task, Assertion] { operator =>
      for {
        existedBefore <- operator.db.existsDatabase(dbName)
        _ <- operator.db.createCollection(dbName, collectionName)
        existsAfter <- operator.db.existsDatabase(dbName)
      } yield {
        existedBefore shouldBe false
        existsAfter shouldBe true
      }
    }
  }

  it should "list collections in the current db" in {
    val db = genNonEmptyStr.sample.get
    val currentColName = genNonEmptyStr.sample.get
    val collectionNames: Seq[String] = Gen.listOfN(10, genNonEmptyStr.map(col => "test-" + col)).sample.get
    val collectionRef = CollectionDocumentRef(db, currentColName)

    MongoConnection.create1(mongoEndpoint, collectionRef).use[Task, Assertion]{ operator =>
      for {
        initialColList <- operator.db.listCollections.toListL
        _ <- Task.traverse(collectionNames)(colName => operator.db.createCollection(colName))
        collectionsList <- operator.db.listCollections.toListL
      } yield {
        //then
        //the specified collection name gets created automatically if it did not existed before
        initialColList shouldBe List(currentColName)
        collectionsList.isEmpty shouldBe false
        collectionsList should contain theSameElementsAs collectionNames.+:(currentColName)
      }
    }
  }


  it should "list collections in the specified db" in {
    val db = genNonEmptyStr.sample.get
    val collectionNames: Seq[String] = Gen.listOfN(10, genNonEmptyStr.map("test-" + _)).sample.get
    val collectionRef = CollectionDocumentRef("randomDb", "randomColName")

    MongoConnection.create1(mongoEndpoint, collectionRef).use[Task, Assertion]{ operator =>
      for {
        _ <- Task.traverse(collectionNames)(colName => operator.db.createCollection(db, colName))
        collectionsList <- operator.db.listCollections(db).toListL
      } yield {
        collectionsList.isEmpty shouldBe false
        collectionsList should contain theSameElementsAs collectionNames
      }
    }
  }

  it should "list database names" in {
    val currentDb = genNonEmptyStr.sample.get
    val collectionRef = CollectionDocumentRef(currentDb, "randomColName")
    val dbNames = Gen.listOfN(5, genNonEmptyStr).sample.get

    MongoConnection.create1(mongoEndpoint, collectionRef).use[Task, Assertion]{ operator =>
      for {
        existedBefore <- operator.db.listDatabases.existsL(dbNames.contains(_))
        _ <- Task.traverse(dbNames)(name => operator.db.createCollection(name, name))
        dbList <- operator.db.listDatabases.toListL
      } yield {
        existedBefore shouldBe false
        dbList.isEmpty shouldBe false
        dbList.count(dbNames.contains(_)) shouldBe dbNames.size
      }
    }
  }




}
