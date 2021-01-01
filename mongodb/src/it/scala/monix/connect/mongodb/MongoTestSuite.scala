/*
 * Copyright (c) 2020-2020 by The Monix Connect Project Developers.
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

import com.mongodb.client.model._
import monix.execution.Scheduler.Implicits.global
import org.bson.conversions.Bson
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class MongoTestSuite extends AnyFlatSpecLike with Fixture with Matchers with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    super.beforeEach()
    MongoDb.dropDatabase(db).runSyncUnsafe()
    MongoDb.dropCollection(db, collectionName).runSyncUnsafe()
  }

  s"${MongoSingle}" should "delete one single element by filtering" in {
    //given
    val exampleName = "deleteOneExample"
    val employees = genEmployeesWith(name = Some(exampleName)).sample.get

    //when
    val r = MongoSource.findOneAndDelete(col, docNameFilter(exampleName)).runSyncUnsafe()

    //then
    r shouldBe None
  }

}
