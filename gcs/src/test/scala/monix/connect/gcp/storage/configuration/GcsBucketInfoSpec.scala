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

package monix.connect.gcp.storage.configuration

import com.google.cloud.ReadChannel
import com.google.cloud.storage.{Bucket, BucketInfo, Storage, Option => _}
import monix.connect.gcp.storage.GscFixture
import org.mockito.IdiomaticMockito
import org.scalacheck.Gen
import org.scalatest.{Assertion, BeforeAndAfterEach}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.jdk.CollectionConverters._

class GcsBucketInfoSpec
  extends AnyWordSpecLike with IdiomaticMockito with Matchers with GscFixture with BeforeAndAfterEach {

  val underlying: Bucket = mock[Bucket]
  val mockStorage: Storage = mock[Storage]
  val readChannel: ReadChannel = mock[ReadChannel]

  override def beforeEach: Unit = {
    super.beforeEach()
    reset(underlying)
  }

  s"${GcsBucketInfo}" can {

    "be created from default java BlobInfo" in {
      //given
      val bucketName = "sampleBucket"
      val bucketInfo: BucketInfo = BucketInfo.newBuilder(bucketName).build()

      //when
      val gcsBlobInfo: GcsBucketInfo = GcsBucketInfo.fromJava(bucketInfo)

      //then
      assertEqualBlobFields(bucketInfo, gcsBlobInfo)
    }

    "be created from method `withMetadata`" in {
      //given
      val bucketName = Gen.alphaLowerStr.sample.get
      val location = GcsBucketInfo.Locations.`ASIA-EAST1`
      val metadata = genBucketInfoMetadata.sample.get

      //when
      val bucketInfo: BucketInfo = GcsBucketInfo.withMetadata(bucketName, location, Some(metadata))

      //then
      Option(bucketInfo.getStorageClass) shouldBe metadata.storageClass
      Option(bucketInfo.getLogging) shouldBe metadata.logging
      Option(bucketInfo.getRetentionPeriod) shouldBe metadata.retentionPeriod.map(_.toMillis)
      Option(bucketInfo.versioningEnabled) shouldBe metadata.versioningEnabled
      Option(bucketInfo.requesterPays) shouldBe metadata.requesterPays
      Option(bucketInfo.getDefaultEventBasedHold) shouldBe metadata.defaultEventBasedHold
      bucketInfo.getAcl shouldBe metadata.acl.asJava
      bucketInfo.getDefaultAcl shouldBe metadata.defaultAcl.asJava
      bucketInfo.getCors shouldBe metadata.cors.asJava
      bucketInfo.getLifecycleRules shouldBe metadata.lifecycleRules.asJava
      Option(bucketInfo.getIamConfiguration) shouldBe metadata.iamConfiguration
      Option(bucketInfo.getDefaultKmsKeyName) shouldBe metadata.defaultKmsKeyName
      bucketInfo.getLabels shouldBe metadata.labels.asJava
      Option(bucketInfo.getIndexPage) shouldBe metadata.indexPage
      Option(bucketInfo.getNotFoundPage) shouldBe metadata.notFoundPage
    }
  }

  def assertEqualBlobFields(bucketInfo: BucketInfo, gcsBucketInfo: GcsBucketInfo): Assertion = {
    bucketInfo.getName shouldBe gcsBucketInfo.name
    bucketInfo.getLocation shouldBe gcsBucketInfo.location
    bucketInfo.getOwner shouldBe gcsBucketInfo.owner
    bucketInfo.getSelfLink shouldBe gcsBucketInfo.selfLink
    Option(bucketInfo.requesterPays) shouldBe gcsBucketInfo.requesterPays
    Option(bucketInfo.versioningEnabled) shouldBe gcsBucketInfo.versioningEnabled
    bucketInfo.getIndexPage shouldBe gcsBucketInfo.indexPage
    bucketInfo.getNotFoundPage shouldBe gcsBucketInfo.notFoundPage
    Option(bucketInfo.getLifecycleRules).getOrElse(List.empty.asJava) shouldBe gcsBucketInfo.lifecycleRules.asJava
    Option(bucketInfo.getStorageClass) shouldBe gcsBucketInfo.storageClass
    bucketInfo.getEtag shouldBe gcsBucketInfo.etag
    Option(bucketInfo.getMetageneration) shouldBe gcsBucketInfo.metageneration
    Option(bucketInfo.getCors).getOrElse(List.empty.asJava) shouldBe gcsBucketInfo.cors.asJava
    Option(bucketInfo.getAcl).getOrElse(List.empty.asJava) shouldBe gcsBucketInfo.acl.asJava
    Option(bucketInfo.getDefaultAcl).getOrElse(List.empty.asJava) shouldBe gcsBucketInfo.defaultAcl.asJava
    Option(bucketInfo.getLabels).getOrElse(Map.empty.asJava) shouldBe gcsBucketInfo.labels.asJava
    bucketInfo.getDefaultKmsKeyName shouldBe gcsBucketInfo.defaultKmsKeyName
    Option(bucketInfo.getDefaultEventBasedHold) shouldBe gcsBucketInfo.defaultEventBasedHold
    Option(bucketInfo.getRetentionEffectiveTime) shouldBe gcsBucketInfo.retentionEffectiveTime
    Option(bucketInfo.retentionPolicyIsLocked) shouldBe gcsBucketInfo.retentionPolicyIsLocked
    Option(bucketInfo.getRetentionPeriod) shouldBe gcsBucketInfo.retentionPeriod
    bucketInfo.getIamConfiguration shouldBe gcsBucketInfo.iamConfiguration
    bucketInfo.getLocationType shouldBe gcsBucketInfo.locationType
    bucketInfo.getGeneratedId shouldBe gcsBucketInfo.generatedId
  }

}
