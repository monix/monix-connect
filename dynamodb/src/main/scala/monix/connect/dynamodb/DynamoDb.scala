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

package monix.connect.dynamodb

import cats.effect.Resource
import monix.connect.aws.auth.MonixAwsConf
import monix.connect.dynamodb.domain.RetryStrategy
import monix.connect.dynamodb.domain.DefaultRetryStrategy
import monix.eval.Task
import monix.execution.annotations.UnsafeBecauseImpure
import monix.reactive.{Consumer, Observable}
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{DynamoDbRequest, DynamoDbResponse}

import scala.concurrent.duration.{Duration, FiniteDuration}

/**
  * An idiomatic DynamoDb client integrated with Monix ecosystem.
  *
  * It is built on top of the [[DynamoDbAsyncClient]], reason why all the exposed methods
  * expect an implicit instance of the client to be in the scope of the call.
  */
object DynamoDb { self =>

  /**
    * Creates a [[Resource]] that will use the values from a
    * configuration file to allocate and release a [[DynamoDb]].
    * Thus, the api expects an `application.conf` file to be present
    * in the `resources` folder.
    *
    * @see how does the expected `.conf` file should look like
    *      https://github.com/monix/monix-connect/blob/master/aws-auth/src/main/resources/reference.conf`
    *
    * @see the cats effect resource data type: https://typelevel.org/cats-effect/datatypes/resource.html
    *
    * @return a [[Resource]] of [[Task]] that allocates and releases a Monix [[DynamoDb]] client.
    */
  def fromConfig: Resource[Task, DynamoDb] = {
    Resource.make {
      for {
        monixAwsConf  <- Task.from(MonixAwsConf.load)
        asyncClient <- Task.now(AsyncClientConversions.fromMonixAwsConf(monixAwsConf))
      } yield {
        self.createUnsafe(asyncClient)
      }
    } { _.close }
  }

  /**
    * Creates a [[Resource]] that will use the passed-by-parameter
    * AWS configurations to acquire and release a Monix [[DynamoDb]] client.
    *
    * ==Example==
    * {{{
    *   import cats.effect.Resource
    *   import monix.eval.Task
    *   import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
    *   import software.amazon.awssdk.regions.Region
    *
    *   val defaultCredentials = DefaultCredentialsProvider.create()
    *   val dynamoDbResource: Resource[Task, DynamoDb] = DynamoDb.create(defaultCredentials, Region.AWS_GLOBAL)
    * }}}
    *
    * @param credentialsProvider the strategy for loading credentials and authenticate to AWS DynamoDb
    * @param region an Amazon Web Services region that hosts a set of Amazon services.
    * @param endpoint the endpoint url with which the SDK should communicate.
    * @param httpClient sets the [[SdkAsyncHttpClient]] that the SDK service client will use to make HTTP calls.
    * @return a [[Resource]] of [[Task]] that allocates and releases a Monix [[DynamoDb]] client.
    **/
  def create(
    credentialsProvider: AwsCredentialsProvider,
    region: Region,
    endpoint: Option[String] = None,
    httpClient: Option[SdkAsyncHttpClient] = None): Resource[Task, DynamoDb] = {
    Resource.make {
      Task.eval {
        val asyncClient = AsyncClientConversions.from(credentialsProvider, region, endpoint, httpClient)
        createUnsafe(asyncClient)
      }
    } { _.close }
  }

  /**
    * Creates a instance of [[DynamoDb]] out of a [[DynamoDbAsyncClient]].
    *
    * It provides a fast forward access to the [[DynamoDb]] that avoids
    * dealing with [[Resource]].
    *
    * Unsafe because the state of the passed [[DynamoDbAsyncClient]] is not guaranteed,
    * it can either be malformed or closed, which would result in underlying failures.
    *
    * @see [[DynamoDb.fromConfig]] and [[DynamoDb.create]] for a pure usage of [[DynamoDb]].
    * They both will make sure that the connection is created with the required
    * resources and guarantee that the client was not previously closed.
    *
    * ==Example==
    * {{{
    *   import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
    *   import software.amazon.awssdk.regions.Region.AWS_GLOBAL
    *   import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
    *
    *   val asyncClient: DynamoDbAsyncClient = DynamoDbAsyncClient
    *      .builder()
    *      .credentialsProvider(DefaultCredentialsProvider.create())
    *      .region(AWS_GLOBAL)
    *      .build()
    *
    *   val dynamoDb: DynamoDb = DynamoDb.createUnsafe(asyncClient)
    * }}}
    *
    * @param dynamoDbAsyncClient an instance of a [[DynamoDbAsyncClient]].
    * @return An instance of the Monix [[DynamoDb]] client
    */
  @UnsafeBecauseImpure
  def createUnsafe(dynamoDbAsyncClient: DynamoDbAsyncClient): DynamoDb = {
    new DynamoDb {
      override val asyncClient: DynamoDbAsyncClient = dynamoDbAsyncClient
    }
  }

  /**
    * Creates a new [[DynamoDb]] instance out of the the passed AWS configurations.
    *
    * It provides a fast forward access to the [[DynamoDb]] that avoids
    * dealing with [[Resource]], however in this case, the created
    * resources will not be released like in [[create]].
    * Thus, it is the user's responsability to close the connection.
    *
    * ==Example==
    * {{{
    *   import monix.execution.Scheduler.Implicits.global
    *   import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
    *   import software.amazon.awssdk.regions.Region
    *
    *   val defaultCred = DefaultCredentialsProvider.create()
    *   val dynamoDb: DynamoDb = DynamoDb.createUnsafe(defaultCred, Region.AWS_GLOBAL)
    *   // do your stuff here
    *   dynamoDb.close.runToFuture
    * }}}
    *
    * @param credentialsProvider Strategy for loading credentials and authenticate to AWS.
    * @param region An Amazon Web Services region that hosts a set of Amazon services.
    * @param endpoint The endpoint url which the SDK should communicate to.
    * @param httpClient Sets the [[SdkAsyncHttpClient]] that the SDK service client will use to make HTTP calls.
    * @return a [[Resource]] of [[Task]] that allocates and releases a Monix [[DynamoDb]] client.
    */
  @UnsafeBecauseImpure
  def createUnsafe(
    credentialsProvider: AwsCredentialsProvider,
    region: Region,
    endpoint: Option[String] = None,
    httpClient: Option[SdkAsyncHttpClient] = None): DynamoDb = {
    val asyncClient = AsyncClientConversions.from(credentialsProvider, region, endpoint, httpClient)
    self.createUnsafe(asyncClient)
  }

  @deprecated("moved to the companion trait as `sink`")
  def consumer[In <: DynamoDbRequest, Out <: DynamoDbResponse](
    retries: Int = 0,
    delayAfterFailure: Option[FiniteDuration] = None)(
    implicit
    dynamoDbOp: DynamoDbOp[In, Out],
    client: DynamoDbAsyncClient): Consumer[In, Unit] = {
    val retryStrategy = RetryStrategy(retries, delayAfterFailure.getOrElse(Duration.Zero))
    DynamoDbSubscriber(DynamoDb.createUnsafe(client), retryStrategy)
  }

  @deprecated("moved to the companion trait for safer usage")
  def transformer[In <: DynamoDbRequest, Out <: DynamoDbResponse](
    retries: Int = 0,
    delayAfterFailure: Option[FiniteDuration] = None)(
    implicit
    dynamoDbOp: DynamoDbOp[In, Out],
    client: DynamoDbAsyncClient): Observable[In] => Observable[Out] = { inObservable: Observable[In] =>
    inObservable.mapEval(request => DynamoDbOp.create(request, retries, delayAfterFailure))
  }

}

/**
  * Represents the Monix DynamoDb client which can
  * be created using the builders from its companion object.
  */
trait DynamoDb { self =>

  private[dynamodb] implicit val asyncClient: DynamoDbAsyncClient

  /**
    * Pre-built [[Consumer]] implementation that expects
    * and executes any subtype [[DynamoDbRequest]].
    * It provides with the flexibility of retrying a
    * failed execution with delay to recover from it.
    *
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @param dynamoDbOp an implicit [[DynamoDbOp]] of the operation that wants to be executed.
    * @return A [[monix.reactive.Consumer]] that expects and executes dynamodb requests.
    */
  def sink[In <: DynamoDbRequest, Out <: DynamoDbResponse](retryStrategy: RetryStrategy = DefaultRetryStrategy)(
    implicit dynamoDbOp: DynamoDbOp[In, Out]): Consumer[In, Unit] =
    DynamoDbSubscriber(self, retryStrategy)

  /**
    * Transformer that executes any given [[DynamoDbRequest]] and transforms
    * them to its corresponding [[DynamoDbResponse]] within [[Task]].
    * It also provides with the flexibility of retrying a failed
    * execution with delay to recover from it.
    *
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests
    * @param dynamoDbOp an implicit [[DynamoDbOp]] of the operation that wants to be executed.
    * @return DynamoDb operation transformer: `Observable[DynamoDbRequest] => Observable[DynamoDbRequest]`.
    */
  def transformer[In <: DynamoDbRequest, Out <: DynamoDbResponse](retryStrategy: RetryStrategy = DefaultRetryStrategy)(
    implicit dynamoDbOp: DynamoDbOp[In, Out]): Observable[In] => Observable[Out] = { inObservable: Observable[In] =>
    inObservable.mapEval(self.single(_, retryStrategy))
  }

  /**
    * Describes a single execution of any [[DynamoDbRequest]] that
    * will return its corresponding [[DynamoDbResponse]].
    *
    * @param request the [[DynamoDbRequest]] that will be executed.
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @param dynamoDbOp an implicit [[DynamoDbOp]] of the operation that wants to be executed.
    * @return A [[Task]] that ends successfully with the response as [[DynamoDbResponse]], or a failed one.
    */
  def single[In <: DynamoDbRequest, Out <: DynamoDbResponse](
    request: In,
    retryStrategy: RetryStrategy = DefaultRetryStrategy)(implicit dynamoDbOp: DynamoDbOp[In, Out]): Task[Out] = {
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    require(retryStrategy.retries >= 0, "Retries per operation must be higher or equal than 0.")
    Task
      .defer(dynamoDbOp(request))
      .onErrorHandleWith { ex =>
        val t = Task
          .defer(
            if (retries > 0) single(request, RetryStrategy(retries - 1, backoffDelay))
            else Task.raiseError(ex))
        backoffDelay match {
          case Duration.Zero => t
          case _ => t.delayExecution(backoffDelay)
        }
      }
  }

  /** Closes the [[asyncClient]] */
  def close: Task[Unit] = Task.eval(asyncClient.close())

}
