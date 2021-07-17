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

package monix.connect.sqs

import monix.connect.aws.auth.{HttpClientConf, MonixAwsConf}
import monix.execution.internal.InternalApi
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsAsyncClient

import java.net.URI
import java.time.Duration

@InternalApi
private[sqs] object AsyncClientConversions {
  self =>

  private[sqs] def fromMonixAwsConf(monixAwsConf: MonixAwsConf): SqsAsyncClient = {
    val builder =
      SqsAsyncClient.builder().credentialsProvider(monixAwsConf.credentials).region(monixAwsConf.region)
    monixAwsConf.httpClient.map(httpConf => builder.httpClient(self.httpConfToClient(httpConf)))
    monixAwsConf.endpoint.map(builder.endpointOverride)
    builder.build()
  }

  private[sqs] def from(
    credentialsProvider: AwsCredentialsProvider,
    region: Region,
    endpoint: Option[String],
    httpClient: Option[SdkAsyncHttpClient]): SqsAsyncClient = {
    val builder = SqsAsyncClient.builder().credentialsProvider(credentialsProvider).region(region)
    httpClient.map(builder.httpClient)
    endpoint.map(uri => builder.endpointOverride(URI.create(uri)))
    builder.build
  }

  private[this] def httpConfToClient(httpClientConf: HttpClientConf): SdkAsyncHttpClient = {
    val builder = NettyNioAsyncHttpClient.builder()
    httpClientConf.connectionMaxIdleTime.map(duration =>
      builder.connectionMaxIdleTime(Duration.ofMillis(duration.toMillis)))
    httpClientConf.connectionTimeToLive.map(duration =>
      builder.connectionTimeToLive(Duration.ofMillis(duration.toMillis)))
    httpClientConf.readTimeout.map(duration => builder.readTimeout(Duration.ofMillis(duration.toMillis)))
    httpClientConf.writeTimeout.map(duration => builder.writeTimeout(Duration.ofMillis(duration.toMillis)))
    httpClientConf.maxConcurrency.map(builder.maxConcurrency(_))
    httpClientConf.maxPendingConnectionAcquires.map(builder.maxPendingConnectionAcquires(_))
    httpClientConf.maxPendingConnectionAcquires.map(builder.maxPendingConnectionAcquires(_))
    builder.build()
  }
}
