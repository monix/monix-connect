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

package monix.connect.hdfs

import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}
import monix.reactive.{Consumer, Observable}
import monix.execution.Scheduler
import monix.testing.scalatest.MonixTaskTest

class HdfsSpec
  extends AsyncWordSpec with MonixTaskTest with Matchers with BeforeAndAfterAll with BeforeAndAfterEach
  with HdfsFixture {

  override implicit val scheduler = Scheduler.io("hdfs-spec")
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

  s"$Hdfs" should {

    "write and read back a single chunk of bytes" in {
      val path: Path = new Path(genFileName.sample.get)
      val hdfsWriter: Consumer[Array[Byte], Long] = Hdfs.write(fs, path)
      val chunk: Array[Byte] = genChunk.sample.get

      for {
        offset <- Observable
          .pure(chunk)
          .consumeWith(hdfsWriter)
        result <- Hdfs.read(fs, path).headL
      } yield {
        result shouldBe chunk
        offset shouldBe chunk.size
      }
    }

    "write and read back multiple chunks" in {
      val path: Path = new Path(genFileName.sample.get)
      val hdfsWriter: Consumer[Array[Byte], Long] = Hdfs.write(fs, path)
      val chunks: List[Array[Byte]] = genChunks.sample.get

      for {
        offset <- Observable
          .from(chunks)
          .consumeWith(hdfsWriter)
        result <- Hdfs.read(fs, path).headL
      } yield {
        result shouldBe chunks.flatten
        offset shouldBe chunks.flatten.size
      }
    }

    "write and read back multiple chunks with a line separator" in {
      val path: Path = new Path(genFileName.sample.get)
      val hdfsWriter: Consumer[Array[Byte], Long] = Hdfs.write(fs, path)
      val chunks: List[Array[Byte]] = genChunks.sample.get

      for {
        offset <- Observable
          .from(chunks)
          .consumeWith(hdfsWriter)
        result <- Hdfs.read(fs, path).headL
        expectedResult: List[Byte] = chunks.flatten
      } yield {
        offset shouldBe chunks.flatten.size
        result shouldBe expectedResult
      }
    }

    "allow to overwrite if enabled" in {
      val path: Path = new Path(genFileName.sample.get)
      val hdfsWriter: Consumer[Array[Byte], Long] = Hdfs.write(fs, path, overwrite = true)
      val chunksA: List[Array[Byte]] = genChunks.sample.get
      val chunksB: List[Array[Byte]] = genChunks.sample.get
      val existedBefore: Boolean = fs.exists(path)

      for {
        offsetA <- Observable.from(chunksA).consumeWith(hdfsWriter)
        resultA <- Hdfs.read(fs, path).headL
        existsA = fs.exists(path)
        offsetB <- Observable
          .from(chunksB)
          .consumeWith(hdfsWriter)
        resultB <- Hdfs.read(fs, path).headL
      } yield {
        existedBefore shouldBe false
        existsA shouldBe true
        resultA shouldBe chunksA.flatten
        offsetA shouldBe chunksA.flatten.size

        fs.exists(path) shouldBe true
        resultB shouldBe chunksB.flatten
        offsetB shouldBe chunksB.flatten.size
      }
    }

    "fail trying to overwrite when in config is disabled, leaving the original file untouched" in {
      val path: Path = new Path(genFileName.sample.get)
      val hdfsWriter: Consumer[Array[Byte], Long] = Hdfs.write(fs, path, overwrite = false)
      val chunksA: List[Array[Byte]] = genChunks.sample.get
      val chunksB: List[Array[Byte]] = genChunks.sample.get
      val existedBefore: Boolean = fs.exists(path)

      for {
        offsetA <- Observable
          .from(chunksA)
          .consumeWith(hdfsWriter)
        resultA <- Hdfs.read(fs, path).headL
        failedOverwriteAttempt <- Observable
          .from(chunksB)
          .consumeWith(hdfsWriter)
          .attempt
        resultAfterOverwriteAttempt <- Hdfs.read(fs, path).headL
      } yield {
        existedBefore shouldBe false
        fs.exists(path) shouldBe true
        resultA shouldBe chunksA.flatten
        offsetA shouldBe chunksA.flatten.size
        failedOverwriteAttempt.isLeft shouldBe true
        failedOverwriteAttempt.left.get shouldBe a[org.apache.hadoop.fs.FileAlreadyExistsException]
        fs.exists(path) shouldBe true
        resultAfterOverwriteAttempt shouldBe chunksA.flatten
        resultAfterOverwriteAttempt.length shouldBe chunksA.flatten.size
      }
    }

    "allow appending to existing files" in {
      val path: Path = new Path(genFileName.sample.get)
      val chunksA: List[Array[Byte]] = genChunks.sample.get
      val chunksB: List[Array[Byte]] = genChunks.sample.get
      val existedBefore: Boolean = fs.exists(path)
      val expectedContent: List[Byte] = (chunksA ++ chunksB).flatten

      for {
        offsetA <- Observable
          .from(chunksA)
          .consumeWith(Hdfs.write(fs, path))
        resultA <- Hdfs.read(fs, path).headL
        fileExistsA = fs.exists(path)

        finalOffset <- Observable
          .from(chunksB)
          .consumeWith(Hdfs.append(fs, path))
        finalResult <- Hdfs.read(fs, path).headL
        finalExists = fs.exists(path)

      } yield {
        existedBefore shouldBe false
        fileExistsA shouldBe true
        resultA shouldBe chunksA.flatten
        offsetA shouldBe chunksA.flatten.size
        finalExists shouldBe true
        finalResult shouldBe expectedContent
        finalOffset shouldBe chunksB.flatten.size
      }
    }

    "fail when appending to a non existing file" in {
      val path: Path = new Path(genFileName.sample.get)
      val chunksA: List[Array[Byte]] = genChunks.sample.get
      val existed: Boolean = fs.exists(path)

      Observable
        .from(chunksA)
        .consumeWith(Hdfs.append(fs, path))
        .attempt
        .asserting { appendAttempt =>
          existed shouldBe false
          appendAttempt.isLeft shouldBe true
        }
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
