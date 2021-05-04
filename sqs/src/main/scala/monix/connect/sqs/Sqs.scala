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
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import monix.connect.aws.auth.MonixAwsConf
import monix.execution.annotations.UnsafeBecauseImpure
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region

object Sqs {
  self =>

  def fromConfig: Resource[Task, Sqs] = {
    Resource.make {
      for {
        clientConf <- MonixAwsConf.load
        asyncClient <- Task.now(AsyncClientConversions.fromMonixAwsConf(clientConf))
      } yield asyncClient
    }(asyncClient => Task.evalAsync(asyncClient.close()))
      .map(self.createUnsafe(_))
  }

  @UnsafeBecauseImpure
  def createUnsafe(implicit sqsAsyncClient: SqsAsyncClient): Sqs = {
    Sqs(SqsConsumer.create, SqsProducer.create, SqsOperator.create)
  }

  @UnsafeBecauseImpure
  def createUnsafe(
                    credentialsProvider: AwsCredentialsProvider,
                    region: Region,
                    endpoint: Option[String] = None,
                    httpClient: Option[SdkAsyncHttpClient] = None): Sqs = {
    val sqsAsyncClient = AsyncClientConversions.from(credentialsProvider, region, endpoint, httpClient)
    self.createUnsafe(sqsAsyncClient)
  }

  def create(
              credentialsProvider: AwsCredentialsProvider,
              region: Region,
              endpoint: Option[String] = None,
              httpClient: Option[SdkAsyncHttpClient] = None): Resource[Task, Sqs] = {

    Resource.make {
      Task.evalAsync {
        AsyncClientConversions.from(credentialsProvider, region, endpoint, httpClient)
      }
    }(asyncClient => Task.evalAsync(asyncClient.close()))
      .map(self.createUnsafe(_))
  }


}

final case class Sqs private[sqs](consumer: SqsConsumer, producer: SqsProducer, operator: SqsOperator)
