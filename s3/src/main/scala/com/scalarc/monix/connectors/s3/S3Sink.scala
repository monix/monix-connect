package com.scalarc.monix.connectors.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{ PutObjectResult }
import monix.reactive.{ Consumer, Observer }
import com.scalarc.monix.connectors.s3.S3Sink.S3Object
import monix.execution.Ack

import scala.util.{ Failure, Success, Try }

protected class S3Sink(s3Client: AmazonS3 = S3Client()) {

  def create(): Consumer[S3Object, Either[Throwable, PutObjectResult]] = {
    Consumer.create[S3Object, Either[Throwable, PutObjectResult]] { (scheduler, cancelable, callback) =>
      new Observer.Sync[S3Object] {
        private var putObjectResult: Either[Throwable, PutObjectResult] = _

        def onNext(s3Object: S3Object): Ack = {
          println("Uploading object to S3 bucket...")
          val S3Object(bucket, key, content) = s3Object
          putObjectResult = Try(s3Client.putObject(bucket, key, content)) match {
            case Success(putObjectResult) => Right(putObjectResult)
            case Failure(exception) => Left(exception)
          }
          monix.execution.Ack.Stop
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

  def putAndForget(): Consumer.Sync[S3Object, Unit] = {
    Consumer.foreach[S3Object] { case S3Object(bucket, key, content) =>
      Try(s3Client.putObject(bucket, key, content)) match {
        case Success(putObjectResult) => {
          println("S3Object upload successfully")
          Right(putObjectResult)
        }
        case Failure(exception) => {
          println("S3Object upload failed with exception:" + exception)
          Left(exception)
        }
      }
    }
  }
}

object S3Sink {
  def apply(): Consumer[S3Object, Either[Throwable, PutObjectResult]] = new S3Sink(S3Client()).create()
  case class S3Object(bucket: String, key: String, content: String)
}
