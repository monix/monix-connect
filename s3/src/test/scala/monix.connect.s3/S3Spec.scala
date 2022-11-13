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

package monix.connect.s3

import monix.eval.Task
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import monix.execution.Scheduler.Implicits.global
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient

class S3Spec extends AnyFlatSpec with BeforeAndAfterEach with Matchers with BeforeAndAfterAll with S3RequestGenerators {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def beforeEach(): Unit = {}

  s"${S3}" should "use default configurable params" in {
    S3.fromConfig().use { s3 => Task(s3 shouldBe a[S3]) }.runSyncUnsafe()
  }

  it should "unsafely be created" in {
    val s3AsyncClient = S3AsyncClient
      .builder()
      .credentialsProvider(AnonymousCredentialsProvider.create())
      .region(Region.EU_SOUTH_1)
      .build()
    S3.createUnsafe(s3AsyncClient) shouldBe a[S3]
  }

  it should "be created from the passed configuration params" in {
    val credentials = AnonymousCredentialsProvider.create()
    S3.create(credentials, Region.AWS_GLOBAL).use { s3 => Task(s3 shouldBe a[S3]) }.runSyncUnsafe()
  }
}
