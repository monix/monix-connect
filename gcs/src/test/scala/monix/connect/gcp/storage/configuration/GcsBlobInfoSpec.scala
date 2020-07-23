package monix.connect.gcp.storage.configuration

import java.nio.charset.StandardCharsets
import java.util.Base64

import com.google.cloud.ReadChannel
import com.google.cloud.storage.{BlobInfo, Storage, Blob => GoogleBlob, Option => _}
import monix.connect.gcp.storage.GscFixture
import org.mockito.IdiomaticMockito
import org.scalatest.matchers.should.Matchers
import org.scalacheck.Gen
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpecLike

import scala.jdk.CollectionConverters._

class GcsBlobInfoSpec extends AnyWordSpecLike with IdiomaticMockito with Matchers with GscFixture {

  val underlying: GoogleBlob = mock[GoogleBlob]
  val mockStorage: Storage = mock[Storage]
  val readChannel: ReadChannel = mock[ReadChannel]

  s"$GcsBlobInfo" can {

    "be created from default java BlobInfo" in {
      //given
      val bucketName = "sampleBucket"
      val blobName = "sampleBlob"
      val blobInfo: BlobInfo = BlobInfo.newBuilder(bucketName, blobName).build()

      //when
      val gcsBlobInfo: GcsBlobInfo = GcsBlobInfo.fromJava(blobInfo)

      //then
      assertEqualBlobFields(blobInfo, gcsBlobInfo)
    }

    "be created from a randomly generated java BlobInfo " in {
      //given
      val blobInfo: BlobInfo = genBlobInfo.sample.get

      //when
      val gcsBlobInfo: GcsBlobInfo = GcsBlobInfo.fromJava(blobInfo)

      //then
      assertEqualBlobFields(blobInfo, gcsBlobInfo)
    }

    "be created from method `withMetadata`" in   {
      //given
      val bucketName = Gen.alphaLowerStr.sample.get
      val blobName = Gen.alphaLowerStr.sample.get
      val metadata = genBlobInfoMetadata.sample.get

      //when
      val blobInfo = GcsBlobInfo.withMetadata(bucketName, blobName, Some(metadata))

      //then
      Option(blobInfo.getContentType) shouldBe metadata.contentType
      Option(blobInfo.getContentDisposition) shouldBe metadata.contentDisposition
      Option(blobInfo.getContentLanguage) shouldBe metadata.contentLanguage
      Option(blobInfo.getContentEncoding) shouldBe metadata.contentEncoding
      Option(blobInfo.getCacheControl) shouldBe metadata.cacheControl
      Option(blobInfo.getCrc32c) shouldBe metadata.crc32c
      Option(blobInfo.getMd5) shouldBe metadata.md5
      Option(blobInfo.getStorageClass) shouldBe metadata.storageClass
      Option(blobInfo.getTemporaryHold) shouldBe metadata.temporaryHold
      Option(blobInfo.getEventBasedHold) shouldBe metadata.eventBasedHold
      blobInfo.getAcl shouldBe metadata.acl.asJava
      blobInfo.getMetadata shouldBe metadata.metadata.asJava
    }
  }

  def assertEqualBlobFields(blobInfo: BlobInfo, gcsBlobInfo: GcsBlobInfo): Assertion = {
    blobInfo.getName shouldBe gcsBlobInfo.name
    blobInfo.getBucket shouldBe gcsBlobInfo.bucket
    Option(blobInfo.getGeneratedId) shouldBe gcsBlobInfo.generatedId
    Option(blobInfo.getSelfLink) shouldBe gcsBlobInfo.selfLink
    Option(blobInfo.getCacheControl) shouldBe gcsBlobInfo.cacheControl
    Option(blobInfo.getAcl).getOrElse(List.empty.asJava) shouldBe gcsBlobInfo.acl.asJava
    Option(blobInfo.getOwner) shouldBe gcsBlobInfo.owner
    Option(blobInfo.getContentType) shouldBe gcsBlobInfo.contentType
    Option(blobInfo.getContentDisposition) shouldBe gcsBlobInfo.contentDisposition
    Option(blobInfo.getContentLanguage) shouldBe gcsBlobInfo.contentLanguage
    Option(blobInfo.getComponentCount) shouldBe gcsBlobInfo.componentCount
    Option(blobInfo.getEtag) shouldBe gcsBlobInfo.etag
    Option(blobInfo.getMd5) shouldBe gcsBlobInfo.md5
    Option(blobInfo.getMd5ToHexString) shouldBe gcsBlobInfo.md5ToHexString
    Option(blobInfo.getCrc32c) shouldBe gcsBlobInfo.crc32c
    Option(blobInfo.getCrc32cToHexString) shouldBe gcsBlobInfo.crc32cToHexString
    Option(blobInfo.getMediaLink) shouldBe gcsBlobInfo.mediaLink
    Option(blobInfo.getMetadata).getOrElse(Map.empty.asJava) shouldBe gcsBlobInfo.metadata.asJava
    Option(blobInfo.getGeneration) shouldBe gcsBlobInfo.generation
    Option(blobInfo.getMetageneration) shouldBe gcsBlobInfo.metageneration
    Option(blobInfo.getDeleteTime) shouldBe gcsBlobInfo.deleteTime
    Option(blobInfo.getUpdateTime) shouldBe gcsBlobInfo.updateTime
    Option(blobInfo.getCreateTime) shouldBe gcsBlobInfo.createTime
    Option(blobInfo.isDirectory) shouldBe gcsBlobInfo.isDirectory
    Option(blobInfo.getCustomerEncryption) shouldBe gcsBlobInfo.customerEncryption
    Option(blobInfo.getStorageClass) shouldBe gcsBlobInfo.storageClass
    Option(blobInfo.getKmsKeyName) shouldBe gcsBlobInfo.kmsKeyName
    Option(blobInfo.getEventBasedHold) shouldBe gcsBlobInfo.eventBasedHold
    Option(blobInfo.getTemporaryHold) shouldBe gcsBlobInfo.temporaryHold
    Option(blobInfo.getRetentionExpirationTime) shouldBe gcsBlobInfo.retentionExpirationTime
  }

}
