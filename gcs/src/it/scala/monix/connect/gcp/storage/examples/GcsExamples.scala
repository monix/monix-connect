package monix.connect.gcp.storage.examples

import java.io.File
import java.nio.file.Files

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import com.google.cloud.storage.{StorageClass, Option => _}
import monix.connect.gcp.storage.configuration.GcsBucketInfo
import monix.connect.gcp.storage.configuration.GcsBucketInfo.Locations
import monix.connect.gcp.storage.{GcsBlob, GcsBucket, GcsStorage}
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.apache.commons.io.FileUtils
import org.scalacheck.Gen
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{BeforeAndAfterAll, Ignore}

@Ignore //this class aggregates some of the examples used for the documentation to easily track and modify them accordingly,
class GcsExamples extends AnyWordSpecLike with Matchers with BeforeAndAfterAll {

  val underlying = LocalStorageHelper.getOptions.getService
  val dir = new File("gcs/tmp").toPath
  val genLocalPath = Gen.identifier.map(s => dir.toAbsolutePath.toString + "/" + s).sample.get
  val testBucketName = Gen.identifier.sample.get

  override def beforeAll(): Unit = {
    FileUtils.deleteDirectory(dir.toFile)
    Files.createDirectory(dir)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.beforeAll()
  }

  s"Gcs consumer implementation" should {

    "bucket donwload" in {
      val storage = GcsStorage(underlying)
      val bucket: Task[Option[GcsBucket]] = storage.getBucket("myBucket")
      val _: Observable[Array[Byte]] = {
        Observable.fromTask(bucket)
          .flatMap {
            case Some(blob) => blob.download("myBlob")
            case None => Observable.empty
          }
      }
    }

    "blob donwload" in {
      import monix.connect.gcp.storage.GcsBlob
      import monix.reactive.Observable

      val storage = GcsStorage.create()
      val memoizedBlob = storage.getBlob("myBucket", "myBlob").memoize

      val _: Observable[Array[Byte]] = {
        for {
          blob <- Observable.fromTask(memoizedBlob): Observable[Option[GcsBlob]]
          bytes <- {
            blob match {
              case Some(blob) => blob.download()
              case None => Observable.empty
            }
          }
        } yield bytes
      }

    }

    "bucket donwload to file" in {
      import java.io.File

      import monix.connect.gcp.storage.GcsBucket
      import monix.eval.Task

      val storage = GcsStorage.create()
      val targetFile = new File("example/target/file.txt")

      val _: Task[Unit] = {
        for {
          maybeBucket <-  storage.getBucket("myBucket"): Task[Option[GcsBucket]]
          _ <- maybeBucket match {
            case Some(bucket) => bucket.downloadToFile("myBlob", targetFile.toPath)
            case None => Task.unit
          }
        } yield ()
      }

    }

    "blob donwload to file" in {
      import java.io.File

      import monix.eval.Task

      val storage = GcsStorage.create()
      val targetFile = new File("example/target/file.txt")
      val _: Task[Unit] = {
        for {
          maybeBucket <- storage.getBlob("myBucket", "myBlob"): Task[Option[GcsBlob]]
          _ <- maybeBucket match {
            case Some(blob) => blob.downloadToFile(targetFile.toPath)
            case None => Task.unit
          }
        } yield ()
      }
    }

    "bucket upload" in {
      import monix.connect.gcp.storage.GcsBucket
      import monix.eval.Task

      val storage = GcsStorage.create()
      val memoizedBucket = storage.createBucket("myBucket", Locations.`EUROPE-WEST1`).memoize
      val ob: Observable[Array[Byte]] = Observable.now("dummy content".getBytes)

      val t: Task[Unit] = for {
        bucket <- memoizedBucket: Task[GcsBucket]
        _ <- ob.consumeWith(bucket.upload("myBlob"))
      } yield ()

      t.runSyncUnsafe()
    }

    "bucket upload from file" in {
      import java.io.File

      import monix.connect.gcp.storage.GcsBucket
      import monix.eval.Task

      val storage = GcsStorage.create()
      val sourceFile = new File("path/to/your/path.txt")

      val _: Task[Unit] = for {
        bucket <- storage.createBucket("myBucket", GcsBucketInfo.Locations.`US-WEST1`): Task[GcsBucket]
        _ <- bucket.uploadFromFile("myBlob", sourceFile.toPath)
      } yield ()
    }

    "blob upload" in {
      import monix.eval.Task
      import monix.execution.Scheduler.Implicits.global

      val storage = GcsStorage.create()
      val memoizedBlob = storage.createBlob("myBucket", "myBlob").memoize

      val ob: Observable[Array[Byte]] = Observable.now("dummy content".getBytes)
      val t: Task[Unit] = for {
        blob <- memoizedBlob
        _ <- ob.consumeWith(blob.upload())
      } yield ()

      t.runToFuture(global)
    }

    "blob upload from file" in {
      import java.io.File

      import monix.connect.gcp.storage.GcsBlob
      import monix.eval.Task

      val storage = GcsStorage.create()
      val sourceFile = new File("path/to/your/path.txt")

      val _: Task[Unit] = for {
        blob <- storage.createBlob("myBucket", "myBlob"): Task[GcsBlob]
        _ <- blob.uploadFromFile(sourceFile.toPath)
      } yield ()
    }

    "create bucket" in {
      import monix.connect.gcp.storage.configuration.GcsBucketInfo
      import monix.connect.gcp.storage.configuration.GcsBucketInfo.Locations
      import monix.connect.gcp.storage.{GcsBucket, GcsStorage}

      val storage = GcsStorage.create()

      val metadata = GcsBucketInfo.Metadata(
        labels = Map(
          "project" -> "my-first-gcs-bucket"
        ),
        storageClass = Some(StorageClass.REGIONAL)
      )
      val _: Task[GcsBucket] = storage.createBucket("mybucket", Locations.`EUROPE-WEST1`, Some(metadata)).memoizeOnSuccess
    }

    "create blob" in {
      import monix.connect.gcp.storage.{GcsBlob, GcsStorage}

      val storage = GcsStorage.create()
      val _: Task[GcsBlob] = storage.createBlob("mybucket","myBlob").memoizeOnSuccess
    }


    "copy blob from underlying" in {
      import monix.connect.gcp.storage.GcsBlob
      import monix.eval.Task

      val storage = GcsStorage.create()
      val sourceBlob: Task[GcsBlob] = storage.createBlob("myBucket", "sourceBlob")
      val _: Task[GcsBlob] =  sourceBlob.flatMap(_.copyTo("targetBlob"))
    }
  }

}
