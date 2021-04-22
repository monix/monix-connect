/*
 * Copyright (c) 2020-2020 by The Monix Connect Project Developers.
 * See the project homepage at: https://monix.io
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

package monix.connect.sqs

import cats.effect.Resource
import monix.eval.Task
import monix.reactive.Observable.Transformer
import monix.reactive.{Consumer, Observable}
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{Message, ReceiveMessageRequest, SqsRequest, SqsResponse}
import monix.connect.aws.auth.AppConf
import monix.execution.annotations.UnsafeBecauseImpure
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region

import scala.jdk.CollectionConverters._

object Sqs {
 self =>

  def fromConfig: Resource[Task, Sqs] = {
    Resource.make {
      for {
        clientConf  <- Task.from(AppConf.load)
        asyncClient <- Task.now(AsyncClientConversions.fromMonixAwsConf(clientConf.monixAws))
      } yield {
        self.createUnsafe(asyncClient)
      }
    } {
      _.close
    }
  }

  @UnsafeBecauseImpure
  def createUnsafe(s3AsyncClient: SqsAsyncClient): Sqs = {
    new Sqs {
      override val sqsAsyncClient: SqsAsyncClient = s3AsyncClient
    }
  }

  @UnsafeBecauseImpure
  def createUnsafe(
                    credentialsProvider: AwsCredentialsProvider,
                    region: Region,
                    endpoint: Option[String] = None,
                    httpClient: Option[SdkAsyncHttpClient] = None): Sqs = {
    val sqsAsyncClient = AsyncClientConversions.from(credentialsProvider, region, endpoint, httpClient)
    self.createUnsafe(s3AsyncClient)
  }


  //todo add settings for batch size, timeout, attributeNames and messageAttributes


}

trait Sqs {

  private[s3] val sqsAsyncClient: SqsAsyncClient

  def source(queueUrl: String): Observable[Message] = {
    for {
      messageRequest <- Observable.repeat[ReceiveMessageRequest](
        ReceiveMessageRequest.builder.queueUrl(queueUrl).build)
      messagesList <- Observable.fromTask(
        Task.from(sqsAsyncClient.receiveMessage(messageRequest)).map(_.messages.asScala.toList))
      message <- Observable.fromIterable(messagesList)
    } yield message
  }

  def sink[In <: SqsRequest, Out <: SqsResponse](implicit sqsOp: SqsOp[In, Out]): Consumer[In, Out] = new SqsSink(sqsOp, sqsAsyncClient)

  def transformer[In <: SqsRequest, Out <: SqsResponse](
                                                         implicit
                                                         sqsOp: SqsOp[In, Out]): Transformer[In, Out] = { inObservable: Observable[In] =>
    inObservable.mapEval(in => sqsOp.execute(in)(sqsAsyncClient))
  }

  def single[In <: SqsRequest, Out <: SqsResponse](request: In)(implicit sqsOp: SqsOp[In, Out]): Task[Out] = sqsOp.execute(request)(sqsAsyncClient)


  /**
    * Closes the underlying [[SqsAsyncClient]].
    */
  def close: Task[Unit] = Task.evalOnce(sqsAsyncClient.close())
}
