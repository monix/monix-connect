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

package monix.connect.hdfs

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}
import monix.reactive.{Consumer, Observable}
import org.scalatest.concurrent.ScalaFutures
import monix.execution.Scheduler.Implicits.global

import scala.util.Try

class HdfsSpec extends AnyWordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with ScalaFutures {

  private var miniHdfs: MiniDFSCluster = _
  private val dir = "./temp/hadoop"
  private val port: Int = 54310
  private val conf = new Configuration()
  conf.set("fs.default.name", s"hdfs://localhost:$port")
  conf.setBoolean("dfs.support.append", true)
  conf.set(
    "dfs.client.block.write.replace-datanode-on-failure.policy",
    "NEVER"
  ) //needed for performing append operation on hadoop-minicluster
  val fs: FileSystem = FileSystem.get(conf)

  s"${Hdfs}" should {

    "write and read back a single chunk of bytes" in new HdfsFixture {
      //given
      val path: Path = new Path(genFileName.sample.get)
      val hdfsWriter: Consumer[Array[Byte], Long] = Hdfs.write(fs, path, lineSeparator = None)
      val chunk: Array[Byte] = genChunk.sample.get

      //when
      val offset = Observable
        .pure(chunk)
        .consumeWith(hdfsWriter)
        .runSyncUnsafe()

      //then
      val r: Array[Byte] = Hdfs.read(fs, path).headL.runSyncUnsafe()
      r shouldBe chunk
      offset shouldBe chunk.size
    }

    "write and read back multiple chunks" in new HdfsFixture {
      //given
      val path: Path = new Path(genFileName.sample.get)
      val hdfsWriter: Consumer[Array[Byte], Long] = Hdfs.write(fs, path, lineSeparator = None)
      val chunks: List[Array[Byte]] = genChunks.sample.get

      //when
      val offset: Long = Observable
        .from(chunks)
        .consumeWith(hdfsWriter)
        .runSyncUnsafe()

      //then
      val r: Array[Byte] = Hdfs.read(fs, path).headL.runSyncUnsafe()
      r shouldBe chunks.flatten
      offset shouldBe chunks.flatten.size
    }

    "write and read back multiple chunks with a line separator" in new HdfsFixture {
      //given
      val path: Path = new Path(genFileName.sample.get)
      val lineSeparator: String = "\n"
      val hdfsWriter: Consumer[Array[Byte], Long] = Hdfs.write(fs, path, lineSeparator = Some(lineSeparator))
      val chunks: List[Array[Byte]] = genChunks.sample.get
      val nChunks = chunks.size

      //when
      val offset: Long = Observable
        .from(chunks)
        .consumeWith(hdfsWriter)
        .runSyncUnsafe()

      //then
      val r: Array[Byte] = Hdfs.read(fs, path).headL.runSyncUnsafe()
      val expectedResult: List[Byte] = chunks.map(_ ++ lineSeparator.getBytes).flatten

      r shouldBe expectedResult
      offset shouldBe (chunks.flatten.size + (nChunks * lineSeparator.getBytes.size))
    }

    "allow to overwrite if enabled" in new HdfsFixture {
      //given
      val path: Path = new Path(genFileName.sample.get)
      val hdfsWriter: Consumer[Array[Byte], Long] = Hdfs.write(fs, path, overwrite = true)
      val chunksA: List[Array[Byte]] = genChunks.sample.get
      val chunksB: List[Array[Byte]] = genChunks.sample.get
      val existedBefore: Boolean = fs.exists(path)

      //when
      val offsetA: Long = Observable
        .from(chunksA)
        .consumeWith(hdfsWriter)
        .runSyncUnsafe()

      //then
      val resultA: Array[Byte] = Hdfs.read(fs, path).headL.runSyncUnsafe()
      existedBefore shouldBe false
      fs.exists(path) shouldBe true
      resultA shouldBe chunksA.flatten
      offsetA shouldBe chunksA.flatten.size

      //and when
      val offsetB: Long = Observable
        .from(chunksB)
        .consumeWith(hdfsWriter)
        .runSyncUnsafe()

      //and then
      val resultB: Array[Byte] = Hdfs.read(fs, path).headL.runSyncUnsafe()
      fs.exists(path) shouldBe true
      resultB shouldBe chunksB.flatten
      offsetB shouldBe chunksB.flatten.size
    }

    "fail (throwing FileAlreadyExistsException) when trying to overwrite when the option is disabled, leaving the original file untouched" in new HdfsFixture {
      //given
      val path: Path = new Path(genFileName.sample.get)
      val hdfsWriter: Consumer[Array[Byte], Long] = Hdfs.write(fs, path, overwrite = false)
      val chunksA: List[Array[Byte]] = genChunks.sample.get
      val chunksB: List[Array[Byte]] = genChunks.sample.get
      val existedBefore: Boolean = fs.exists(path)

      //when
      val offsetA: Long = Observable
        .from(chunksA)
        .consumeWith(hdfsWriter)
        .runSyncUnsafe()

      //then
      val resultA: Array[Byte] = Hdfs.read(fs, path).headL.runSyncUnsafe()
      existedBefore shouldBe false
      fs.exists(path) shouldBe true
      resultA shouldBe chunksA.flatten
      offsetA shouldBe chunksA.flatten.size

      //and when
      val failedOverwriteAttemt: Try[Long] = Try {
        Observable
          .from(chunksB)
          .consumeWith(hdfsWriter)
          .runSyncUnsafe() //it should throw an `org.apache.hadoop.fs.FileAlreadyExistsException`
      }

      //and then
      val overwritten: Array[Byte] = Hdfs.read(fs, path).headL.runSyncUnsafe()
      failedOverwriteAttemt.isFailure shouldBe true
      fs.exists(path) shouldBe true
      overwritten shouldBe chunksA.flatten
      overwritten.size shouldBe chunksA.flatten.size
    }

    "allow appending to existing files" in new HdfsFixture {
      //given
      val path: Path = new Path(genFileName.sample.get)
      val chunksA: List[Array[Byte]] = genChunks.sample.get
      val chunksB: List[Array[Byte]] = genChunks.sample.get
      val existedBefore: Boolean = fs.exists(path)
      val expectedContent: List[Byte] = (chunksA ++ chunksB).flatten

      //when
      val offsetA: Long = Observable
        .from(chunksA)
        .consumeWith(Hdfs.write(fs, path))
        .runSyncUnsafe()

      //then
      val resultA: Array[Byte] = Hdfs.read(fs, path).headL.runSyncUnsafe()
      existedBefore shouldBe false
      fs.exists(path) shouldBe true
      resultA shouldBe chunksA.flatten
      offsetA shouldBe chunksA.flatten.size

      //and when
      val finalOffset: Long = Observable
        .from(chunksB)
        .consumeWith(Hdfs.append(fs, path))
        .runSyncUnsafe()

      //and then
      val finalResult: Array[Byte] = Hdfs.read(fs, path).headL.runSyncUnsafe()
      fs.exists(path) shouldBe true
      finalResult shouldBe expectedContent
      finalOffset shouldBe chunksB.flatten.size
    }

    "fail when appending to a non existing file" in new HdfsFixture {
      //given
      val path: Path = new Path(genFileName.sample.get)
      val chunksA: List[Array[Byte]] = genChunks.sample.get
      val existed: Boolean = fs.exists(path)

      //when
      val appendingFailure: Try[Long] = Try {
        Observable
          .from(chunksA)
          .consumeWith(Hdfs.append(fs, path))
          .runSyncUnsafe()
      }

      //then
      existed shouldBe false
      appendingFailure.isFailure shouldBe true
    }

  }

  override protected def beforeAll(): Unit = {
    val baseDir: File = new File(dir, "test")
    val miniDfsConf: HdfsConfiguration = new HdfsConfiguration
    miniDfsConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
    miniHdfs = new MiniDFSCluster.Builder(miniDfsConf)
      .nameNodePort(port)
      .format(true)
      .build()
    miniHdfs.waitClusterUp()
  }

  override protected def afterAll(): Unit = {
    fs.close()
    miniHdfs.shutdown()
  }

  override protected def afterEach(): Unit = {
    fs.delete(new Path(dir), true)
  }
}
