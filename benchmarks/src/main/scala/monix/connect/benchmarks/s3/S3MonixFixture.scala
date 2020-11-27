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

package monix.connect.benchmarks.s3

import java.net.URI
import java.time.Duration

import monix.connect.s3.S3
import monix.eval.Coeval
import monix.execution.Scheduler
import org.scalacheck.Gen
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient

import scala.util.{Failure, Success, Try}

trait S3MonixFixture {

  val minioEndPoint: String = "http://localhost:9000"

  val s3AccessKey: String = "TESTKEY"
  val s3SecretKey: String = "TESTSECRET"

  val httpClient = NettyNioAsyncHttpClient
    .builder()
    .maxConcurrency(500)
    .maxPendingConnectionAcquires(50000)
    .connectionAcquisitionTimeout(Duration.ofSeconds(60))
    .readTimeout(Duration.ofSeconds(60))
    .build();

  val basicAWSCredentials = AwsBasicCredentials.create(s3AccessKey, s3SecretKey)
  val staticCredProvider = StaticCredentialsProvider.create(basicAWSCredentials)
  implicit val s3AsyncClient: S3AsyncClient = S3AsyncClient
    .builder()
    .httpClient(httpClient)
    .credentialsProvider(staticCredProvider)
    .region(Region.AWS_GLOBAL)
    .endpointOverride(URI.create(minioEndPoint))
    .build

  protected val s3Resource = S3.create(staticCredProvider, Region.AWS_GLOBAL, Some(minioEndPoint), Some(httpClient))

  def createBucket(bucketName: String)(implicit scheduler: Scheduler) = {
    Try(s3Resource.use(_.createBucket(bucketName)).runSyncUnsafe()) match {
      case Success(_) => println(s"Created S3 bucket ${bucketName} ")
      case Failure(e) => println(s"Failed to create S3 bucket ${bucketName} with exception: ${e.getMessage}")
    }
  }
}
