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

import cats.effect.Resource
import monix.eval.Task
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import monix.connect.aws.auth.MonixAwsConf
import monix.connect.sqs.producer.SqsProducer
import monix.connect.sqs.consumer.SqsConsumer
import monix.execution.annotations.UnsafeBecauseImpure
import pureconfig.{KebabCase, NamingConvention}
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region

object Sqs {

  /**
    * Provides a resource that uses the values from the
    * config file to acquire and release a [[Sqs]] instance.
    *
    * It does not requires any input parameter as it expect the
    * aws config to be set from `application.conf`, which has to be
    * placed under the `resources` folder and will overwrite the
    * defaults values from:
    * `https://github.com/monix/monix-connect/blob/master/aws-auth/src/main/resources/reference.conf`.
    *
    * ==Example==
    * {{{
    *   import monix.connect.aws.auth.MonixAwsConf
    *   import monix.connect.sqs.Sqs
    *
    *   Sqs.fromConfig.use { sqs =>
    *      //business logic here
    *      Task.unit
    *   }
    * }}}
    *
    * @return a [[Resource]] of [[Task]] that acquires and releases a [[Sqs]] connection.
    */
  @deprecated("Use `fromConfig(namingConvention)`", "0.6.1")
  def fromConfig: Resource[Task, Sqs] = {
    Resource.make {
      for {
        clientConf  <- MonixAwsConf.load()
        asyncClient <- Task.now(AsyncClientConversions.fromMonixAwsConf(clientConf))
      } yield asyncClient
    }(asyncClient => Task.evalAsync(asyncClient.close()))
      .map(this.createUnsafe(_))
  }

  /**
    * Provides a resource that uses the values from the
    * config file to acquire and release a [[Sqs]] instance.
    *
    * It does not requires any input parameter as it expect the
    * aws config to be set from `application.conf`, which has to be
    * placed under the `resources` folder and will overwrite the
    * defaults values from:
    * `https://github.com/monix/monix-connect/blob/master/aws-auth/src/main/resources/reference.conf`.
    *
    * ==Example==
    * {{{
    *   import monix.connect.aws.auth.MonixAwsConf
    *   import monix.connect.sqs.Sqs
    *
    *   Sqs.fromConfig.use { sqs =>
    *      //business logic here
    *      Task.unit
    *   }
    * }}}
    *
    * @return a [[Resource]] of [[Task]] that acquires and releases a [[Sqs]] connection.
    */
  def fromConfig(namingConvention: NamingConvention = KebabCase): Resource[Task, Sqs] = {
    Resource.make {
      for {
        clientConf  <- MonixAwsConf.load(namingConvention)
        asyncClient <- Task.now(AsyncClientConversions.fromMonixAwsConf(clientConf))
      } yield asyncClient
    }(asyncClient => Task.evalAsync(asyncClient.close()))
      .map(this.createUnsafe(_))
  }

  /**
    * Provides a resource that uses the values from the
    * config file to acquire and release a [[Sqs]] instance.
    *
    * The config will come from [[MonixAwsConf]] which it is
    * actually obtained from the `application.conf` file present
    * under the `resources` folder.
    *
    * ==Example==
    * {{{
    *   import monix.connect.aws.auth.MonixAwsConf
    *   import monix.connect.sqs.Sqs
    *   import software.amazon.awssdk.regions.Region
    *
    *   MonixAwsConf.load().flatMap{ awsConf =>
    *       // you can update your config from code too
    *       val updatedAwsConf = awsConf.copy(region = Region.AP_SOUTH_1)
    *       Sqs.fromConfig(updatedAwsConf).use { sqs =>
    *          //business logic here
    *          Task.unit
    *       }
    *   }
    * }}}
    *
    * @param monixAwsConf the monix aws config read from config file.
    *
    * @return a [[Resource]] of [[Task]] that acquires and releases a [[Sqs]] connection.
    */
  def fromConfig(monixAwsConf: MonixAwsConf): Resource[Task, Sqs] = {
    Resource.make {
     Task.now(AsyncClientConversions.fromMonixAwsConf(monixAwsConf))
    }(asyncClient => Task.evalAsync(asyncClient.close()))
      .map(this.createUnsafe(_))
  }

  /**
    * Provides a resource that uses the values from the
    * config file to acquire and release a [[Sqs]] instance.
    *
    * The config will come from [[MonixAwsConf]] which it is
    * actually obtained from the `application.conf` file present
    * under the `resources` folder.
    *
    * ==Example==
    * {{{
    *   import monix.connect.aws.auth.MonixAwsConf
    *   import monix.eval.Task
    *   import monix.connect.sqs.Sqs
    *
    *   val monixAwsConf: Task[MonixAwsConf] = MonixAwsConf.load()
    *   Sqs.fromConfig(awsConf).use { sqs =>
    *          //business logic here
    *          Task.unit
    *    }
    * }}}
    *
    * @param monixAwsConf a task containing the monix aws config
    *                     read from config file.
    *
    * @return a [[Resource]] of [[Task]] that acquires and releases a [[Sqs]] connection.
    */
  def fromConfig(monixAwsConf: Task[MonixAwsConf]): Resource[Task, Sqs] = {
    Resource.make {
      monixAwsConf.map(AsyncClientConversions.fromMonixAwsConf)
    }(asyncClient => Task.evalAsync(asyncClient.close()))
      .map(this.createUnsafe(_))
  }

  /**
    * Creates a [[Resource]] that using passed-by-parameter
    * AWS configurations, it encodes the acquisition and release
    * of the resources needed to instantiate the [[Sqs]].
    *
    * ==Example==
    *
    * {{{
    *   import cats.effect.Resource
    *   import monix.eval.Task
    *   import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
    *   import software.amazon.awssdk.regions.Region
    *
    *   val defaultCredentials = DefaultCredentialsProvider.create()
    *   val sqsResource: Resource[Task, Sqs] = Sqs.create(defaultCredentials, Region.AWS_GLOBAL)
    * }}}
    *
    * @param credentialsProvider strategy for loading credentials and authenticate to AWS Sqs
    * @param region              an Amazon Web Services region that hosts a set of Amazon services.
    * @param endpoint            the endpoint with which the SDK should communicate.
    * @param httpClient          sets the [[SdkAsyncHttpClient]] that the SDK service client will use to make HTTP calls.
    * @return a [[Resource]] of [[Task]] that acquires and releases [[Sqs]] connection.
    */

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
      .map(this.createUnsafe(_))
  }

  /**
    * Creates a instance of [[Sqs]] out of a [[SqsAsyncClient]]
    * which provides a fast forward access to the [[Sqs]] that avoids
    * dealing with a proper resource and usage.
    *
    * Unsafe because the state of the passed [[SqsAsyncClient]] is not guaranteed,
    * it can either be malformed or closed, which would result in underlying failures.
    *
    * ==Example==
    *
    * {{{
    *   import java.time.Duration
    *   import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
    *   import software.amazon.awssdk.regions.Region.AWS_GLOBAL
    *   import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
    *   import software.amazon.awssdk.services.sqs.SqsAsyncClient
    *
    *   // the exceptions related with concurrency or timeouts from any of the requests
    *   // might be solved by raising the `maxConcurrency`, `maxPendingConnectionAcquire` or
    *   // `connectionAcquisitionTimeout` from the underlying netty http async client.
    *   // see below an example on how to increase such values.
    *
    *   val httpClient = NettyNioAsyncHttpClient.builder()
    *     .maxConcurrency(500)
    *     .maxPendingConnectionAcquires(50000)
    *     .connectionAcquisitionTimeout(Duration.ofSeconds(60))
    *     .readTimeout(Duration.ofSeconds(60))
    *     .build()
    *
    *   val sqsAsyncClient: SqsAsyncClient = SqsAsyncClient
    *     .builder()
    *     .httpClient(httpClient)
    *     .credentialsProvider(DefaultCredentialsProvider.create())
    *     .region(AWS_GLOBAL)
    *     .build()
    *
    *     val sqs: Sqs = Sqs.createUnsafe(sqsAsyncClient)
    * }}}
    *
    * @see [[Sqs.fromConfig]] and [[Sqs.create]] for a pure implementation.
    *      They both will make sure that the s3 connection is created with the required
    *      resources and guarantee that the client was not previously closed.
    *
    * @param sqsAsyncClient an instance of a [[SqsAsyncClient]].
    * @return an instance of [[Sqs]]
    */
  @UnsafeBecauseImpure
  def createUnsafe(implicit sqsAsyncClient: SqsAsyncClient): Sqs = {
    Sqs(SqsOperator.create, SqsProducer.create, SqsConsumer.create)
  }

  /**
    * Creates a new [[Sqs]] instance out of the the passed AWS configurations.
    *
    * It provides a fast forward access to the [[Sqs]] instance.
    * Thus, it is the user's responsibility of the user to do a proper resource usage
    * and so, closing the connection.
    *
    * ==Example==
    *
    * {{{
    *   import monix.execution.Scheduler.Implicits.global
    *   import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
    *   import software.amazon.awssdk.regions.Region
    *
    *   val defaultCred = DefaultCredentialsProvider.create()
    *   val sqs: Sqs = Sqs.createUnsafe(defaultCred, Region.AWS_GLOBAL)
    *   // do your stuff here
    *   sqs.close.runToFuture
    * }}}
    *
    * @param credentialsProvider Strategy for loading credentials and authenticate to AWS S3
    * @param region              An Amazon Web Services region that hosts a set of Amazon services.
    * @param endpoint            The endpoint with which the SDK should communicate.
    * @param httpClient          Sets the [[SdkAsyncHttpClient]] that the SDK service client will use to make HTTP calls.
    * @return a [[Resource]] of [[Task]] that allocates and releases [[Sqs]].
    */
  @UnsafeBecauseImpure
  def createUnsafe(
    credentialsProvider: AwsCredentialsProvider,
    region: Region,
    endpoint: Option[String] = None,
    httpClient: Option[SdkAsyncHttpClient] = None): Sqs = {
    val sqsAsyncClient = AsyncClientConversions.from(credentialsProvider, region, endpoint, httpClient)
    createUnsafe(sqsAsyncClient)
  }

}

case class Sqs private[sqs] (operator: SqsOperator, producer: SqsProducer, consumer: SqsConsumer) {
  def close: Task[Unit] = Task.parZip3(operator.close, producer.close, consumer.close).void
}
