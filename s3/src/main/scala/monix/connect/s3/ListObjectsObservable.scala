package monix.connect.s3

import monix.eval.Task
import monix.execution.{Ack, Cancelable}
import monix.execution.internal.InternalApi
import monix.reactive.Observable
import monix.reactive.observers.Subscriber
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{ListObjectsV2Request, ListObjectsV2Response, RequestPayer}

@InternalApi
private[s3] class ListObjectsObservable(
  bucket: String,
  prefix: Option[String] = None,
  maxTotalKeys: Option[Int] = None,
  requestPayer: Option[RequestPayer] = None,
  s3AsyncClient: S3AsyncClient)
  extends Observable[ListObjectsV2Response] {
  self =>

  require(maxTotalKeys.getOrElse(1) > 0, "The max number of keys, if defined, needs to be higher or equal than 1.")
  private[self] val firstRequestSize = maxTotalKeys.map(maxKeys => math.min(maxKeys, domain.awsDefaulMaxKeysList))
  private[self] val initialRequest: ListObjectsV2Request =
    S3RequestBuilder.listObjectsV2(bucket, prefix = prefix, maxKeys = firstRequestSize, requestPayer = requestPayer)

  def unsafeSubscribeFn(subscriber: Subscriber[ListObjectsV2Response]): Cancelable = {
    val s = subscriber.scheduler
    nextListRequest(subscriber, maxTotalKeys, initialRequest).runToFuture(s)
  }

  private[self] def prepareNextRequest(continuationToken: String, pendingKeys: Option[Int]): ListObjectsV2Request = {
    val requestBuilder = initialRequest.toBuilder.continuationToken(continuationToken)
    pendingKeys.map { n =>
      val nextMaxkeys = math.min(n, domain.awsDefaulMaxKeysList)
      requestBuilder.maxKeys(nextMaxkeys)
    }.getOrElse(domain.awsDefaulMaxKeysList)
    requestBuilder.build()
  }

  private[self] def nextListRequest(
    sub: Subscriber[ListObjectsV2Response],
    pendingKeys: Option[Int],
    request: ListObjectsV2Request): Task[Unit] = {

    for {
      r <- {
        Task.from(s3AsyncClient.listObjectsV2(request)).onErrorHandleWith { ex =>
          sub.onError(ex)
          Task.raiseError(ex)
        }
      }
      ack <- Task.deferFuture(sub.onNext(r))
      nextRequest <- {
        ack match {
          case Ack.Continue => {
            if (r.isTruncated && (r.nextContinuationToken != null)) {
              val updatedPendingKeys = pendingKeys.map(_ - r.contents.size)
              updatedPendingKeys match {
                case Some(pendingKeys) =>
                  if (pendingKeys <= 0) { sub.onComplete(); Task.unit }
                  else
                    nextListRequest(
                      sub,
                      updatedPendingKeys,
                      prepareNextRequest(r.nextContinuationToken, updatedPendingKeys))
                case None =>
                  nextListRequest(sub, Option.empty[Int], prepareNextRequest(r.nextContinuationToken, None))
              }
            } else {
              sub.onComplete()
              Task.unit
            }
          }
          case Ack.Stop => Task.unit
        }
      }
    } yield nextRequest
  }

}

object ListObjectsObservable {
  def apply(
    bucket: String,
    prefix: Option[String],
    maxTotalKeys: Option[Int],
    requestPayer: Option[RequestPayer],
    s3AsyncClient: S3AsyncClient): ListObjectsObservable =
    new ListObjectsObservable(bucket, prefix, maxTotalKeys, requestPayer, s3AsyncClient)
}
