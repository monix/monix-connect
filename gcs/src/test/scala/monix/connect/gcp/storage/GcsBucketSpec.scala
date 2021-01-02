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

import java.util

import com.google.cloud.storage.Bucket.BucketSourceOption
import com.google.cloud.storage.Storage.BucketTargetOption
import com.google.cloud.storage.{Acl, Bucket, Option => _}
import monix.execution.Scheduler.Implicits.global
import org.mockito.Mockito.{times, verify}
import org.mockito.MockitoSugar.when
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class GcsBucketSpec
  extends AnyWordSpecLike with IdiomaticMockito with Matchers with ArgumentMatchersSugar with GscFixture
  with BeforeAndAfterEach {

  val underlying: Bucket = mock[Bucket]
  val bucket: GcsBucket = GcsBucket(underlying)

  override def beforeEach: Unit = {
    super.beforeEach()
    reset(underlying)
  }

  s"$GcsBucket" should {

    "implement a exists operation" in {
      //given
      val bucketSourceOption: BucketSourceOption = mock[BucketSourceOption]
      when(underlying.exists(bucketSourceOption)).thenAnswer(true)

      //when
      val result: Boolean = bucket.exists(bucketSourceOption).runSyncUnsafe()

      //then
      result shouldBe true
      verify(underlying, times(1)).exists(bucketSourceOption)
    }

    "implement reload method" that {
      "correctly returns some bucket" in {
        //given
        val bucketSourceOption: BucketSourceOption = mock[BucketSourceOption]
        val googleBucket = mock[Bucket]
        when(underlying.reload(bucketSourceOption)).thenReturn(googleBucket)

        //when
        val maybeBucket: Option[GcsBucket] = bucket.reload(bucketSourceOption).runSyncUnsafe()

        //then
        maybeBucket.isDefined shouldBe true
        maybeBucket.get shouldBe a[GcsBucket]
        verify(underlying, times(1)).reload(bucketSourceOption)
      }

      "safely returns none whenever the underlying response was null" in {
        //given
        val bucketSourceOption: BucketSourceOption = mock[BucketSourceOption]
        when(underlying.reload(bucketSourceOption)).thenReturn(null)

        //when
        val maybeBucket: Option[GcsBucket] = bucket.reload(bucketSourceOption).runSyncUnsafe()

        //then
        maybeBucket.isDefined shouldBe false
        verify(underlying, times(1)).reload(bucketSourceOption)
      }
    }

    "implement a update operation that correctly returns some bucket" in {
      // given
      val bucketTargetOption: BucketTargetOption = mock[BucketTargetOption]
      val googleBucket = mock[Bucket]
      when(underlying.update(bucketTargetOption)).thenReturn(googleBucket)

      //when
      val maybeBucket: GcsBucket = bucket.update(bucketTargetOption).runSyncUnsafe()

      //then
      maybeBucket shouldBe a[GcsBucket]
      verify(underlying, times(1)).update(bucketTargetOption)
    }

    "implement an async delete operation" in {
      // given
      val bucketSourceOption: BucketSourceOption = mock[BucketSourceOption]
      when(underlying.delete(bucketSourceOption)).thenReturn(true)

      //when
      val maybeBucket: Boolean = bucket.delete(bucketSourceOption).runSyncUnsafe()

      //then
      maybeBucket shouldBe true
      verify(underlying, times(1)).delete(bucketSourceOption)
    }

    "implement async acl operations" that {

      "implements a create operation that correctly returns some acl" in {
        // given
        val acl = genAcl.sample.get
        when(underlying.createAcl(acl)).thenReturn(acl)

        //when
        val maybeAcl: Acl = bucket.createAcl(acl).runSyncUnsafe()

        //then
        maybeAcl shouldBe a[Acl]
        verify(underlying, times(1)).createAcl(acl)
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
          verify(underlying, times(1)).getAcl(acl)
        }

        "that correctly returns some acl" in {
          // given
          val acl = genAcl.sample.get
          when(underlying.getAcl(acl.getEntity)).thenReturn(acl)

          //when
          val maybeAcl: Option[Acl] = bucket.getAcl(acl.getEntity).runSyncUnsafe()

          //then
          maybeAcl.isDefined shouldBe true
          maybeAcl.get shouldBe a[Acl]
          verify(underlying, times(1)).getAcl(acl.getEntity)
        }
      }

      "implements an update operation that correctly returns some acl" in {
        // given
        val acl = genAcl.sample.get
        when(underlying.updateAcl(acl)).thenReturn(acl)

        //when
        val maybeAcl: Acl = bucket.updateAcl(acl).runSyncUnsafe()

        //then
        maybeAcl shouldBe a[Acl]
        verify(underlying, times(1)).updateAcl(acl)
      }

      "implements a delete operation that deletes the specified acl" in {
        // given
        val aclEntity = genAcl.sample.get.getEntity
        when(underlying.deleteAcl(aclEntity)).thenReturn(true)

        //when
        val result: Boolean = bucket.deleteAcl(aclEntity).runSyncUnsafe()

        //then
        result shouldBe true
        verify(underlying, times(1)).deleteAcl(aclEntity)
      }

      "implement a list acl operation that correctly returns zero or more acls" in {
        // given
        val acl = genAcl.sample.get
        val acls = util.Arrays.asList(acl, acl, acl)
        when(underlying.listAcls()).thenReturn(acls)

        //when
        val result: List[Acl] = bucket.listAcls().toListL.runSyncUnsafe()

        //then
        result shouldBe List(acl, acl, acl)
        verify(underlying, times(1)).listAcls()
      }

      "implement a create defaultAcl operation that correctly returns some defaultAcl" in {
        // given
        val acl = genAcl.sample.get
        when(underlying.createDefaultAcl(acl)).thenReturn(acl)

        //when
        val maybeAcl: Acl = bucket.createDefaultAcl(acl).runSyncUnsafe()

        //then
        maybeAcl shouldBe a[Acl]
        verify(underlying, times(1)).createDefaultAcl(acl)
      }
    }

    "implement async defaultAcl operations" that {

      "implement a get operation" that {

        "safely returns none whenever the underlying response was null" in {
          // given
          val aclEntity = genAcl.sample.get.getEntity
          when(underlying.getDefaultAcl(aclEntity)).thenReturn(null)

          //when
          val maybeAcl: Option[Acl] = bucket.getDefaultAcl(aclEntity).runSyncUnsafe()

          //then
          maybeAcl.isDefined shouldBe false
          verify(underlying, times(1)).getDefaultAcl(aclEntity)
        }

        "correctly returns some defaultAcl" in {
          // given
          val acl = genAcl.sample.get
          when(underlying.getDefaultAcl(acl.getEntity)).thenReturn(acl)

          //when
          val maybeAcl: Option[Acl] = bucket.getDefaultAcl(acl.getEntity).runSyncUnsafe()

          //then
          maybeAcl.isDefined shouldBe true
          maybeAcl.get shouldBe a[Acl]
          verify(underlying, times(1)).getDefaultAcl(acl.getEntity)
        }
      }

      "implement an update operation that correctly returns some defaultAcl" in {
        // given
        val acl = genAcl.sample.get
        when(underlying.updateDefaultAcl(acl)).thenReturn(acl)

        //when
        val maybeAcl: Acl = bucket.updateDefaultAcl(acl).runSyncUnsafe()

        //then
        maybeAcl shouldBe a[Acl]
        verify(underlying, times(1)).updateDefaultAcl(acl)
      }

      "implement a delete operation that deletes the specified defaultAcl" in {
        // given
        val aclEntity = genAcl.sample.get.getEntity
        when(underlying.deleteDefaultAcl(aclEntity)).thenReturn(true)

        //when
        val result: Boolean = bucket.deleteDefaultAcl(aclEntity).runSyncUnsafe()

        //then
        result shouldBe true
        verify(underlying, times(1)).deleteDefaultAcl(aclEntity)
      }

      "implement a list defaultAcl operation that correctly returns zero or more acl" in {
        // given
        val acl = genAcl.sample.get
        val acls = util.Arrays.asList(acl, acl, acl)
        when(underlying.listDefaultAcls()).thenReturn(acls)

        //when
        val result: List[Acl] = bucket.listDefaultAcls().toListL.runSyncUnsafe()

        //then
        result shouldBe List(acl, acl, acl)
        verify(underlying, times(1)).listDefaultAcls()
      }
    }

    "implement a lock retention policy operation" in {
      // given
      val bucketTargetOption = mock[BucketTargetOption]
      when(underlying.lockRetentionPolicy(bucketTargetOption)).thenReturn(underlying)

      //when
      val maybeBucket: GcsBucket = bucket.lockRetentionPolicy(bucketTargetOption).runSyncUnsafe()

      //then
      maybeBucket shouldBe a[GcsBucket]
      verify(underlying, times(1)).lockRetentionPolicy(bucketTargetOption)
    }
  }
}
