package monix.connect.gcs

import java.util

import com.google.cloud.storage.Bucket.BucketSourceOption
import com.google.cloud.storage.Storage.BucketTargetOption
import com.google.cloud.storage.{Acl, Bucket => GoogleBucket, Option => _}
import monix.execution.Scheduler.Implicits.global
import org.mockito.IdiomaticMockito
import org.mockito.MockitoSugar.when
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class BucketSpec extends AnyWordSpecLike with IdiomaticMockito with Matchers {

  val underlying: GoogleBucket = mock[GoogleBucket]
  val bucket: Bucket = Bucket(underlying)

  "implement an async exists operation" in {
    //given
    val bucketSourceOption: BucketSourceOption = mock[BucketSourceOption]
    when(underlying.exists(bucketSourceOption)).thenAnswer(true)

    //when
    val result: Boolean = bucket.exists(bucketSourceOption).runSyncUnsafe()

    //then
    result shouldBe true
  }

  "implement reload method" that {

    "correctly returns some bucket" in {
      //given
      val bucketSourceOption: BucketSourceOption = mock[BucketSourceOption]
      val googleBucket = mock[GoogleBucket]
      when(underlying.reload(bucketSourceOption)).thenReturn(googleBucket)

      //when
      val maybeBucket: Option[Bucket] = bucket.reload(bucketSourceOption).runSyncUnsafe()

      //then
      maybeBucket.isDefined shouldBe true
      maybeBucket.get shouldBe a[Bucket]
    }

    "safely returns none whenever the underlying response was null" in {
      //given
      val bucketSourceOption: BucketSourceOption = mock[BucketSourceOption]
      when(underlying.reload(bucketSourceOption)).thenReturn(null)

      //when
      val maybeBucket: Option[Bucket] = bucket.reload(bucketSourceOption).runSyncUnsafe()

      //then
      maybeBucket.isDefined shouldBe false
    }
  }

  "implement an async update operation that correctly returns some bucket" in {
    // given
    val bucketTargetOption: BucketTargetOption = mock[BucketTargetOption]
    val googleBucket = mock[GoogleBucket]
    when(underlying.update(bucketTargetOption)).thenReturn(googleBucket)

    //when
    val maybeBucket: Bucket = bucket.update(bucketTargetOption).runSyncUnsafe()

    //then
    maybeBucket shouldBe a[Bucket]
  }

  "implement an async delete operation" in {
    // given
    val bucketSourceOption: BucketSourceOption = mock[BucketSourceOption]
    when(underlying.delete(bucketSourceOption)).thenReturn(true)

    //when
    val maybeBucket: Boolean = bucket.delete(bucketSourceOption).runSyncUnsafe()

    //then
    maybeBucket shouldBe true
  }
  

  "implement async acl operations" that {

    "implements a create operation that correctly returns some acl" in {
      // given
      val acl = mock[Acl]
      when(underlying.createAcl(acl)).thenReturn(acl)

      //when
      val maybeAcl: Acl = bucket.createAcl(acl).runSyncUnsafe()

      //then
      maybeAcl shouldBe a[Acl]
    }

    "implements a get operation" that {

      "that safely returns none whenever the underlying response was null" in {
        // given
        val acl = mock[Acl.Entity]
        when(underlying.getAcl(acl)).thenReturn(null)

        //when
        val maybeAcl: Option[Acl] = bucket.getAcl(acl).runSyncUnsafe()

        //then
        maybeAcl.isDefined shouldBe false
      }

      "that correctly returns some acl" in {
        // given
        val aclEntity = mock[Acl.Entity]
        val acl = mock[Acl]
        when(underlying.getAcl(aclEntity)).thenReturn(acl)

        //when
        val maybeAcl: Option[Acl] = bucket.getAcl(aclEntity).runSyncUnsafe()

        //then
        maybeAcl.isDefined shouldBe true
        maybeAcl.get shouldBe a[Acl]
      }
    }

    "implements an update operation that correctly returns some acl" in {
      // given
      val acl = mock[Acl]
      when(underlying.updateAcl(acl)).thenReturn(acl)

      //when
      val maybeAcl: Acl = bucket.updateAcl(acl).runSyncUnsafe()

      //then
      maybeAcl shouldBe a[Acl]
    }

    "implements a delete operation that deletes the specified acl" in {
      // given
      val acl = mock[Acl.Entity]
      when(underlying.deleteAcl(acl)).thenReturn(true)

      //when
      val result: Boolean = bucket.deleteAcl(acl).runSyncUnsafe()

      //then
      result shouldBe true
    }

    "implement an async list acl operation that correctly returns zero or more acls" in {
      // given
      val acl = mock[Acl]
      val acls = util.Arrays.asList(acl, acl, acl)
      when(underlying.listAcls()).thenReturn(acls)

      //when
      val result: List[Acl] = bucket.listAcls().toListL.runSyncUnsafe()

      //then
      result shouldBe List(acl, acl, acl)
    }

    "implement an async create defaultAcl operation that correctly returns some defaultAcl" in {
      // given
      val acl = mock[Acl]
      when(underlying.createDefaultAcl(acl)).thenReturn(acl)

      //when
      val maybeAcl: Acl = bucket.createDefaultAcl(acl).runSyncUnsafe()

      //then
      maybeAcl shouldBe a[Acl]
    }
  }

  "implement async defaultAcl operations" that {

    "implement a get operation" that {

      "that safely returns none whenever the underlying response was null" in {
        // given
        val acl = mock[Acl.Entity]
        when(underlying.getDefaultAcl(acl)).thenReturn(null)

        //when
        val maybeAcl: Option[Acl] = bucket.getDefaultAcl(acl).runSyncUnsafe()

        //then
        maybeAcl.isDefined shouldBe false
      }

      "that correctly returns some defaultAcl" in {
        // given
        val aclEntity = mock[Acl.Entity]
        val acl = mock[Acl]
        when(underlying.getDefaultAcl(aclEntity)).thenReturn(acl)

        //when
        val maybeAcl: Option[Acl] = bucket.getDefaultAcl(aclEntity).runSyncUnsafe()

        //then
        maybeAcl.isDefined shouldBe true
        maybeAcl.get shouldBe a[Acl]
      }
    }

    "implement an update operation that correctly returns some defaultAcl" in {
      // given
      val acl = mock[Acl]
      when(underlying.updateDefaultAcl(acl)).thenReturn(acl)

      //when
      val maybeAcl: Acl = bucket.updateDefaultAcl(acl).runSyncUnsafe()

      //then
      maybeAcl shouldBe a[Acl]
    }

    "implement a delete operation that deletes the specified defaultAcl" in {
      // given
      val acl = mock[Acl.Entity]
      when(underlying.deleteDefaultAcl(acl)).thenReturn(true)

      //when
      val result: Boolean = bucket.deleteDefaultAcl(acl).runSyncUnsafe()

      //then
      result shouldBe true
    }

    "implement a list defaultAcl operation that correctly returns zero or more defaultAcl's" in {
      // given
      val acl = mock[Acl]
      val acls = util.Arrays.asList(acl, acl, acl)
      when(underlying.listDefaultAcls()).thenReturn(acls)

      //when
      val result: List[Acl] = bucket.listDefaultAcls().toListL.runSyncUnsafe()

      //then
      result shouldBe List(acl, acl, acl)
    }
  }
  
  "implement an async lock retention policy operation" in {
    // given
    val bucketTargetOption = mock[BucketTargetOption]
    when(underlying.lockRetentionPolicy(bucketTargetOption)).thenReturn(underlying)

    //when
    val maybeBucket: Bucket = bucket.lockRetentionPolicy(bucketTargetOption).runSyncUnsafe()

    //then
    maybeBucket shouldBe a[Bucket]
    
  }
}