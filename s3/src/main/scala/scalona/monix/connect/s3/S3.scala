package scalona.monix.connect.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.PutObjectResult
import monix.reactive.{Consumer, Observer}
import monix.execution.Ack
import monix.eval.Task
import scalona.monix.connect.s3.domain.{Done, S3Object}

import scala.util.{Failure, Success, Try}

private[dynamodb] class S3(s3Client: AmazonS3) {

  def sink: Consumer[S3Object, Either[Throwable, PutObjectResult]] = {
    Consumer.create[S3Object, Either[Throwable, PutObjectResult]] { (_, _, callback) =>
      new Observer.Sync[S3Object] {
        private var putObjectResult: Either[Throwable, PutObjectResult] = _

        def onNext(s3Object: S3Object): Ack = {
          val S3Object(bucket, key, content) = s3Object
          putObjectResult = Try(s3Client.putObject(bucket, key, content)) match {
            case Success(putResult) => Right(putResult)
            case Failure(exception) => Left(exception)
          }
          monix.execution.Ack.Continue
        }

        def onComplete(): Unit = {
          callback.onSuccess(putObjectResult)
        }

        def onError(ex: Throwable): Unit = {
          callback.onError(ex)
        }
      }
    }
  }

  def getObjectAsString(bucket: String, key: String): Task[String] = {
    Task(s3Client.getObjectAsString(bucket, key))
  }
}

object S3 {
  def apply(s3Client: AmazonS3 = S3Client()): S3 = new S3(s3Client)
}
