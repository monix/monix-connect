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

package monix.connect.gcp.storage

import com.google.cloud.storage.Acl._
import com.google.cloud.storage.BucketInfo.LifecycleRule.{LifecycleAction, LifecycleCondition}
import com.google.cloud.storage.BucketInfo.{IamConfiguration, LifecycleRule, Logging}
import com.google.cloud.storage._
import monix.connect.gcp.storage.configuration.{GcsBlobInfo, GcsBucketInfo}
import org.scalacheck.Gen

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

trait GscFixture {

  val genBool: Gen[Boolean] = Gen.oneOf(true, false)
  val genNonEmtyStr: Gen[String] = Gen.identifier.sample.get
  val genAcl: Gen[Acl] = for {
    entity <- Gen
      .oneOf[Entity](User.ofAllUsers(), new Group("sample@email.com"), new Project(Project.ProjectRole.OWNERS, "id"))
    role <- Gen.oneOf(Role.OWNER, Role.READER, Role.WRITER)
  } yield {
    Acl.of(entity, role)
  }

  val genBlobId: Gen[BlobId] = for {
    bucket <- genNonEmtyStr
    name <- genNonEmtyStr
  } yield BlobId.of(bucket, name)

  val genStorageClass: Gen[StorageClass] = Gen.oneOf(
    StorageClass.ARCHIVE,
    StorageClass.COLDLINE,
    StorageClass.DURABLE_REDUCED_AVAILABILITY,
    StorageClass.MULTI_REGIONAL,
    StorageClass.NEARLINE,
    StorageClass.REGIONAL,
    StorageClass.STANDARD
  )

  val genBlobInfo: Gen[BlobInfo] = for {
    bucket <- Gen.alphaLowerStr
    name <- Gen.alphaLowerStr
    contentType <- Gen.option(Gen.alphaLowerStr)
    contentDisposition <- Gen.option(Gen.alphaLowerStr)
    contentLanguage <- Gen.option(Gen.alphaLowerStr)
    contentEncoding <- Gen.option(Gen.alphaLowerStr)
    cacheControl <- Gen.option(Gen.alphaLowerStr)
    crc32c <- Gen.option(Gen.alphaLowerStr.map(_.hashCode.toString))
    crc32cFromHexString <- Gen.option("1100")
    md5 <- Gen.option(Gen.alphaLowerStr.map(_.hashCode.toString))
    md5FromHexString <- Gen.some("0001")
    storageClass <- Gen.option(genStorageClass)
    temporaryHold <- Gen.option(Gen.oneOf(true, false))
    eventBasedHold <- Gen.option(Gen.oneOf(true, false))
    acl <- Gen.listOf(genAcl)
    metadata <- Gen.mapOfN(3, ("k", "v"))
  } yield {
    val builder = BlobInfo.newBuilder(BlobId.of(bucket, name))
    contentType.foreach(builder.setContentType)
    contentDisposition.foreach(builder.setContentDisposition)
    contentLanguage.foreach(builder.setContentLanguage)
    contentEncoding.foreach(builder.setContentEncoding)
    cacheControl.foreach(builder.setCacheControl)
    crc32c.foreach(builder.setCrc32c)
    crc32cFromHexString.foreach(builder.setCrc32cFromHexString)
    md5.foreach(builder.setMd5)
    md5FromHexString.foreach(builder.setMd5FromHexString)
    storageClass.foreach(builder.setStorageClass)
    temporaryHold.foreach(builder.setEventBasedHold(_))
    eventBasedHold.foreach(b => builder.setEventBasedHold(b))
    builder.setAcl(acl.asJava)
    builder.setMetadata(metadata.asJava)
    builder.build()
  }

  val genBlobInfoMetadata: Gen[GcsBlobInfo.Metadata] = for {
    contentType <- Gen.option(Gen.alphaLowerStr)
    contentDisposition <- Gen.option(Gen.alphaLowerStr)
    contentLanguage <- Gen.option(Gen.alphaLowerStr)
    contentEncoding <- Gen.option(Gen.alphaLowerStr)
    cacheControl <- Gen.option(Gen.alphaLowerStr)
    crc32c <- Gen.option(Gen.alphaLowerStr)
    md5 <- Gen.option(Gen.alphaLowerStr)
    storageClass <- Gen.option(genStorageClass)
    temporaryHold <- Gen.option(Gen.oneOf(true, false))
    eventBasedHold <- Gen.option(Gen.oneOf(true, false))
  } yield {
    GcsBlobInfo.Metadata(
      contentType = contentType,
      contentDisposition = contentDisposition,
      contentLanguage = contentLanguage,
      contentEncoding = contentEncoding,
      cacheControl = cacheControl,
      crc32c = crc32c,
      crc32cFromHexString = None,
      md5 = md5,
      md5FromHexString = None,
      storageClass = storageClass,
      temporaryHold = temporaryHold,
      eventBasedHold = eventBasedHold
    )
  }

  val genIamConf = IamConfiguration.newBuilder().build()
  val genDeleteLifeCycleRule =
    new LifecycleRule(LifecycleAction.newDeleteAction(), LifecycleCondition.newBuilder().setIsLive(true).build())
  val genLifeCycleRules = Gen.nonEmptyListOf(genDeleteLifeCycleRule)
  val genCors = Gen.nonEmptyListOf(Cors.newBuilder().build())
  val genBucketInfoMetadata = for {
    storageClass <- Gen.option(genStorageClass)
    logging <- Gen.option(Logging.newBuilder().setLogBucket("WARN").build())
    retentionPeriod <- Gen.option(Gen.choose(1, 1000).map(_.seconds))
    versioningEnabled <- Gen.option(genBool)
    requesterPays <- Gen.option(genBool)
    eventBasedHold <- Gen.option(genBool)
    acl <- Gen.listOf(genAcl)
    defaultAcl <- Gen.listOf(genAcl)
    cors <- genCors
    lifecycleRules <- genLifeCycleRules
    iamConfiguration <- Gen.option(genIamConf)
    defaultKmsKeyName <- Gen.option(Gen.alphaLowerStr)
    labels <- Gen.mapOfN(3, ("labelKey", "labelValue"))
    indexPage <- Gen.option(Gen.alphaLowerStr)
    notFoundPage <- Gen.option(Gen.alphaLowerStr)
  } yield {
    GcsBucketInfo.Metadata(
      storageClass = storageClass,
      logging = logging,
      retentionPeriod = retentionPeriod,
      versioningEnabled = versioningEnabled,
      requesterPays = requesterPays,
      defaultEventBasedHold = eventBasedHold,
      acl = acl,
      defaultAcl = defaultAcl,
      cors = cors,
      lifecycleRules = lifecycleRules,
      iamConfiguration = iamConfiguration,
      defaultKmsKeyName = defaultKmsKeyName,
      labels = labels,
      indexPage = indexPage,
      notFoundPage = notFoundPage
    )
  }
}
