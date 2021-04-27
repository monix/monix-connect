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
import software.amazon.awssdk.services.sqs.model.{AddPermissionRequest, ChangeMessageVisibilityRequest, CreateQueueRequest, DeleteMessageRequest, DeleteQueueRequest, GetQueueAttributesRequest, GetQueueUrlRequest, ListDeadLetterSourceQueuesRequest, ListQueueTagsRequest, ListQueuesRequest, Message, PurgeQueueRequest, QueueAttributeName, QueueDoesNotExistException, ReceiveMessageRequest, SendMessageBatchRequest, SendMessageRequest, SendMessageResponse, SetQueueAttributesRequest, SqsRequest, SqsResponse, TagQueueRequest, UntagQueueRequest}
import monix.connect.aws.auth.MonixAwsConf
import monix.connect.sqs.domain.{QueueMessage, QueueName, QueueUrl, ReceivedMessage}
import monix.execution.annotations.UnsafeBecauseImpure
import monix.execution.internal.InternalApi
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region

import java.util.UUID
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters._

object Sqs {
  self =>

  def fromConfig: Resource[Task, Sqs] = {
    Resource.make {
      for {
        clientConf <- Task.from(MonixAwsConf.load)
        asyncClient <- Task.now(AsyncClientConversions.fromMonixAwsConf(clientConf))
      } yield {
        self.createUnsafe(asyncClient)
      }
    } {
      _.close
    }
  }

  @UnsafeBecauseImpure
  def createUnsafe(sqsAsyncClient: SqsAsyncClient): Sqs = {
    new Sqs {
      override val asyncClient: SqsAsyncClient = sqsAsyncClient
    }
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
      }.map(self.createUnsafe)
    } {
      _.close
    }
  }
}

trait Sqs {

  private[sqs] implicit val asyncClient: SqsAsyncClient

  /**
    *
    * @param queueUrl
    * @return
    */
  def receiveManualAck(queueUrl: QueueUrl,
                       maxMessages: Int = 10,
                       visibilityTimeout: FiniteDuration = 30.seconds,
                       waitTimeSeconds: FiniteDuration = Duration.Zero): Observable[ReceivedMessage] = {
    for {
      receiveRequest <- Observable.repeat(
        receiveRequest(queueUrl, maxMessages, waitTimeSeconds, visibilityTimeout))
      receivedMessages <- Observable.fromTask(
        SqsOp.receiveMessage.execute(receiveRequest).map(_.messages.asScala.toList))
      message <- Observable.fromIterable(receivedMessages)
        .map(msg => new ReceivedMessage(this, queueUrl, msg))
    } yield message
  }

  def receiveAutoAck(queueUrl: QueueUrl,
                     visibilityTimeout: FiniteDuration = 30.seconds,
                     waitTimeSeconds: FiniteDuration = Duration.Zero): Observable[Message] = {
    receiveManualAck(queueUrl,
      maxMessages = 10,
      visibilityTimeout = visibilityTimeout,
      waitTimeSeconds = waitTimeSeconds)
      .doOnNextF(_.deleteFromQueue())
      .map(_.message)
  }

  private def receiveRequest(queueUrl: QueueUrl,
                             maxMessages: Int,
                             visibilityTimeout: FiniteDuration,
                             waitTimeSeconds: FiniteDuration = Duration.Zero): ReceiveMessageRequest = {
    val builder = ReceiveMessageRequest.builder
      .queueUrl(queueUrl.url)
      // always set to the maximum value for achieving the most performance
      .maxNumberOfMessages(maxMessages)
      .attributeNames(QueueAttributeName.ALL)
      .visibilityTimeout(visibilityTimeout.toSeconds.toInt)
    if (waitTimeSeconds != Duration.Zero) builder.waitTimeSeconds(waitTimeSeconds.toSeconds.toInt)
    builder.build
  }

  def sink[Req <: SqsRequest, Resp <: SqsResponse](stopOnError: Boolean = false)(implicit sqsOp: SqsOp[Req, Resp]): Consumer[Req, Unit] =
    new SqsSink[Req, Req, Resp](in => in, sqsOp, asyncClient, stopOnError)

  def addPermission(queueUrl: String, actions: List[String], awsAccountIds: List[String],
                    label: String): Task[Unit] = {
    val addPermissionReq = AddPermissionRequest.builder
      .queueUrl(queueUrl)
      .actions(actions: _*)
      .awsAccountIds(awsAccountIds: _*)
      .label(label).build
    SqsOp.addPermission.execute(addPermissionReq)(asyncClient).void
  }

  def changeMessageVisibility(queueUrl: String, receiptHandle: String,
                              visibilityTimeout: FiniteDuration): Task[Unit] = {
    val changeMessageVisibilityRequest = ChangeMessageVisibilityRequest.builder
      .queueUrl(queueUrl)
      .receiptHandle(receiptHandle)
      .visibilityTimeout(visibilityTimeout.toSeconds.toInt).build
    SqsOp.changeMessageVisibility.execute(changeMessageVisibilityRequest)(asyncClient).void
  }

  def deleteQueue(queueUrl: QueueUrl): Task[Unit] = {
    val deleteRequest = DeleteQueueRequest.builder.queueUrl(queueUrl.url).build
    SqsOp.deleteQueue.execute(deleteRequest)(asyncClient).void
  }

  def createQueue(queueName: QueueName,
                  tags: Map[String, String] = Map.empty,
                  attributes: Map[QueueAttributeName, String] = Map.empty): Task[QueueUrl] = {
    val createQueueRequest = CreateQueueRequest.builder
      .queueName(queueName.name)
      .tags(tags.asJava)
      .attributes(attributes.asJava).build
    SqsOp.createQueue.execute(createQueueRequest)(asyncClient)
      .map(response => QueueUrl(response.queueUrl))
  }

  def deleteMessage(queueUrl: QueueUrl, receiptHandle: String): Task[Unit] = {
    val deleteMessageRequest = DeleteMessageRequest.builder
      .queueUrl(queueUrl.url)
      .receiptHandle(receiptHandle).build
    SqsOp.deleteMessage.execute(deleteMessageRequest)(asyncClient).void
  }

  def existsQueue(queueName: QueueName, queueOwnerAWSAccountId: Option[String] = None): Task[Boolean] =
    getQueueUrl(queueName, queueOwnerAWSAccountId).map(_.isDefined)

  def getQueueUrl(queueName: QueueName,
                  queueOwnerAWSAccountId: Option[String] = None): Task[Option[QueueUrl]] = {
    queueUrlOrFail(queueName, queueOwnerAWSAccountId)
      .map(Option(_))
      .onErrorHandleWith { ex =>
        if (ex.isInstanceOf[QueueDoesNotExistException]) {
          Task.now(Option.empty)
        } else {
          Task.raiseError(ex)
        }
      }
  }

  @InternalApi
  private def queueUrlOrFail(queueName: QueueName,
                             queueOwnerAWSAccountId: Option[String] = None): Task[QueueUrl] = {
    val requestBuilder = GetQueueUrlRequest.builder.queueName(queueName.name)
    queueOwnerAWSAccountId.map(requestBuilder.queueOwnerAWSAccountId)
    SqsOp.getQueueUrl.execute(requestBuilder.build)(asyncClient).map(r => QueueUrl(r.queueUrl))
  }

  def getQueueAttributes(queueUrl: QueueUrl): Task[Map[QueueAttributeName, String]] = {
    val getAttributesRequest = GetQueueAttributesRequest.builder.queueUrl(queueUrl.url).attributeNames(QueueAttributeName.ALL).build
    SqsOp.getQueueAttributes.execute(getAttributesRequest)(asyncClient)
      .map(_.attributes.asScala.toMap)
  }

  //todo test
  def listDeadLetterQueueUrls(queueUrl: QueueUrl): Observable[QueueUrl] = {
    val listRequest = ListDeadLetterSourceQueuesRequest.builder.queueUrl(queueUrl.url).build
    for {
      listResponse <- Observable.fromTask(SqsOp.listDeadLetter.execute(listRequest)(asyncClient))
      queueUrl <- Observable.fromIterable(listResponse.queueUrls.asScala).map(QueueUrl)
    } yield queueUrl
  }

  def listAllQueueUrls: Observable[String] = {
    for {
      listResponse <- Observable.fromReactivePublisher(asyncClient.listQueuesPaginator)
      queueUrl <- Observable.fromIterable(listResponse.queueUrls.asScala)
    } yield queueUrl
  }

  def listQueueUrls(queuePrefix: String): Observable[String] = {
    val listRequest = ListQueuesRequest.builder.queueNamePrefix(queuePrefix).build
    for {
      listResponse <- Observable.fromTask(SqsOp.listQueues.execute(listRequest)(asyncClient))
      queueUrl <- Observable.fromIterable(listResponse.queueUrls.asScala)
    } yield queueUrl
  }

  def listQueueTags(queueUrl: QueueUrl): Task[Map[String, String]] = {
    val listRequest = ListQueueTagsRequest.builder.queueUrl(queueUrl.url).build
    SqsOp.listQueueTags.execute(listRequest)(asyncClient).map(_.tags.asScala.toMap)
  }

  //PurgeQueueInProgressException, QueueDeletedRecentlyException, QueueDoesNotExistException, QueueNameExistsException
  def purgeQueue(queueUrl: QueueUrl): Task[Unit] = {
    val purgeQueue = PurgeQueueRequest.builder.queueUrl(queueUrl.url).build
    SqsOp.purgeQueue.execute(purgeQueue)(asyncClient).void
  }

  def sendMessage(queueMessage: QueueMessage,
                  queueUrl: QueueUrl,
                  groupId: Option[String] = None,
                  delayDuration: Option[FiniteDuration] = None): Task[SendMessageResponse] = {
    val message = queueMessage.toMessageRequest(queueUrl, groupId, delayDuration)
    SqsOp.sendMessage.execute(message)(asyncClient)
  }

  def sendMessageSink(queueUrl: QueueUrl,
                      groupId: Option[String] = None,
                      delayDuration: Option[FiniteDuration] = None,
                      stopOnError: Boolean = false): Consumer[QueueMessage, Unit] = {
    val toJavaMessage = (message: QueueMessage) => message.toMessageRequest(queueUrl, groupId, delayDuration)
    new SqsSink(toJavaMessage, SqsOp.sendMessage, asyncClient, stopOnError)
  }

  def sendBatchMessagesSink(queueUrl: String,
                            groupId: Option[String] = None,
                            delayDuration: Option[FiniteDuration] = None,
                            stopOnError: Boolean = false): Consumer[List[QueueMessage], Unit] = {
    val messagesAsBatch: (List[QueueMessage]) => SendMessageBatchRequest = { messages =>
      val batchEntries = messages.map(_.toMessageBatchEntry(UUID.randomUUID().toString, groupId, delayDuration))
      SendMessageBatchRequest.builder.entries(batchEntries.asJava).queueUrl(queueUrl).build
    }
    new SqsSink(messagesAsBatch, SqsOp.sendMessageBatch, asyncClient, stopOnError)
  }

  def setQueueAttributes(queueUrl: QueueUrl,
                         attributes: Map[QueueAttributeName, String]): Task[Unit] = {
    val setAttributesRequest = SetQueueAttributesRequest.builder.queueUrl(queueUrl.url).attributes(attributes.asJava).build
    SqsOp.setQueueAttributes.execute(setAttributesRequest)(asyncClient).void
  }

  def tagQueue(queueUrl: QueueUrl, tags: Map[String, String]): Task[Unit] = {
    val tagQueueRequest = TagQueueRequest.builder.queueUrl(queueUrl.url).tags(tags.asJava).build
    SqsOp.tagQueue.execute(tagQueueRequest)(asyncClient).void
  }

  def untagQueue(queueName: QueueName, tagKeys: List[String]): Task[Unit] = {
    for {
      queueUrl <- getQueueUrl(queueName)
      _ <- queueUrl.map(untagQueue(_, tagKeys)).getOrElse(Task.unit)
    } yield ()
  }

  def untagQueue(queueUrl: QueueUrl, tagKeys: List[String]): Task[Unit] = {
    val untagQueueRequest = UntagQueueRequest.builder.queueUrl(queueUrl.url).tagKeys(tagKeys.asJava).build
    SqsOp.untagQueue.execute(untagQueueRequest)(asyncClient).void
  }

  def transformer[In <: SqsRequest, Out <: SqsResponse](
                                                         implicit
                                                         sqsOp: SqsOp[In, Out]): Transformer[In, Out] = { inObservable: Observable[In] =>
    inObservable.mapEval(in => sqsOp.execute(in)(asyncClient))
  }

  def single[In <: SqsRequest, Out <: SqsResponse](request: In)(implicit sqsOp: SqsOp[In, Out]): Task[Out] = sqsOp.execute(request)(asyncClient)


  /**
    * Closes the underlying [[SqsAsyncClient]].
    */
  def close: Task[Unit] = Task.evalOnce(asyncClient.close())
}
