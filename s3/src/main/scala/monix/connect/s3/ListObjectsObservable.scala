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

package monix.connect.s3

import monix.eval.Task
import monix.execution.{Ack, Cancelable}
import monix.execution.internal.InternalApi
import monix.reactive.Observable
import monix.reactive.observers.Subscriber
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{ListObjectsV2Request, ListObjectsV2Response, RequestPayer, S3Object}

import scala.jdk.CollectionConverters._

@InternalApi
private[s3] class ListObjectsObservable(
  bucket: String,
  prefix: Option[String] = None,
  maxTotalKeys: Option[Int] = None,
  requestPayer: Option[RequestPayer] = None,
  s3AsyncClient: S3AsyncClient)
  extends Observable[ListObjectsV2Response] {

  //if the max num. of keys is empty, it lets the aws library to default it, otherwise it will be maximum 1000
  private[this] val firstRequestSize = maxTotalKeys.map(maxKeys => math.min(maxKeys, domain.awsDefaultMaxKeysList))
  private[this] val initialRequest: ListObjectsV2Request =
    S3RequestBuilder.listObjectsV2(bucket, prefix = prefix, maxKeys = firstRequestSize, requestPayer = requestPayer)

  def unsafeSubscribeFn(subscriber: Subscriber[ListObjectsV2Response]): Cancelable = {
    val s = subscriber.scheduler
    if (maxTotalKeys.getOrElse(1) > 0) {
      nextListRequest(subscriber, maxTotalKeys, initialRequest).runToFuture(s)
    } else {
      subscriber.onError(
        new IllegalArgumentException(s"The max number of keys, if defined, needs to be higher or equal than 1."))
      Cancelable.empty
    }
  }

  private[this] def prepareNextRequest(continuationToken: String, pendingKeys: Option[Int]): ListObjectsV2Request = {
    val requestBuilder = initialRequest.toBuilder.continuationToken(continuationToken)
    pendingKeys.map { n =>
      val nextMaxKeys = math.min(n, domain.awsDefaultMaxKeysList)
      requestBuilder.maxKeys(nextMaxKeys)
    }
    requestBuilder.build()
  }

  private[this] def nextListRequest(
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

private[s3] object ListObjectsObservable {
  def apply(
    bucket: String,
    prefix: Option[String],
    maxTotalKeys: Option[Int],
    requestPayer: Option[RequestPayer],
    s3AsyncClient: S3AsyncClient): ListObjectsObservable =
    new ListObjectsObservable(bucket, prefix, maxTotalKeys, requestPayer, s3AsyncClient)

  def listNHelper(
    bucket: String,
    n: Int,
    sort: (S3Object, S3Object) => Boolean,
    prefix: Option[String] = None,
    requestPayer: Option[RequestPayer] = None,
    s3AsyncClient: S3AsyncClient): Observable[S3Object] = {
    for {
      listResponse <- ListObjectsObservable(bucket, prefix, None, None, s3AsyncClient).foldLeft(List.empty[S3Object])(
        (prev, curr) => {
          (prev ++ curr.contents.asScala).sortWith(sort).take(n)
        })
      s3Object <- Observable.fromIterable(listResponse)
    } yield s3Object
  }
}
