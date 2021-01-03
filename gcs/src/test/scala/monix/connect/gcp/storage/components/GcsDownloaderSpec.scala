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

package monix.connect.gcp.storage.components

import com.google.cloud.ReadChannel
import com.google.cloud.storage.{BlobId, Storage, Blob => GoogleBlob, Option => _}
import monix.execution.Scheduler.Implicits.global
import org.mockito.IdiomaticMockito
import org.mockito.MockitoSugar.{doNothing, when}
import org.scalatest.matchers.should.Matchers
import org.mockito.Mockito.{times, verify}
import org.mockito.ArgumentMatchers.any
import org.scalatest.{BeforeAndAfterEach, Ignore}
import org.scalatest.wordspec.AnyWordSpecLike

@Ignore
class GcsDownloaderSpec extends AnyWordSpecLike with IdiomaticMockito with Matchers with BeforeAndAfterEach {

  val underlying: GoogleBlob = mock[GoogleBlob]
  val mockStorage: Storage = mock[Storage]
  val readChannel: ReadChannel = mock[ReadChannel]

  override def beforeEach: Unit = {
    super.beforeEach()
    reset(underlying)
  }

  s"GcsDownloader" should {

    "download a blob" in new GcsDownloader {
      //given
      val bucket = "sampleBucket"
      val blobName = "sampleBlob"
      val chunkSize = 100
      when(mockStorage.reader(bucket, blobName)).thenReturn(readChannel)
      doNothing.when(readChannel).setChunkSize(chunkSize)

      //when
      download(mockStorage, BlobId.of(bucket, blobName), chunkSize).toListL.runSyncUnsafe().flatten

      //then
      verify(mockStorage, times(1)).reader(bucket, blobName)
      verify(readChannel, times(1)).read(any())
      verify(readChannel, times(1)).close()
    }

  }
}
