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
import software.amazon.awssdk.services.sqs.model.{AddPermissionRequest, ChangeMessageVisibilityRequest, CreateQueueRequest, DeleteMessageRequest, DeleteQueueRequest, GetQueueUrlRequest, ListDeadLetterSourceQueuesRequest, ListQueueTagsRequest, ListQueuesRequest, Message, PurgeQueueRequest, QueueAttributeName, QueueDoesNotExistException, ReceiveMessageRequest, SendMessageBatchRequest, SendMessageRequest, SendMessageResponse, SetQueueAttributesRequest, SqsRequest, SqsResponse, TagQueueRequest}
import monix.connect.aws.auth.MonixAwsConf
import monix.execution.annotations.UnsafeBecauseImpure
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region

import java.util.UUID
import scala.concurrent.duration.FiniteDuration
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

  private[sqs] val asyncClient: SqsAsyncClient

  /**
    *
    * @param queueUrl
    * @return
    */
  def receiveMessages(queueUrl: String): Observable[Message] = {
    for {
      messageRequest <- Observable.repeat[ReceiveMessageRequest](
        ReceiveMessageRequest.builder.queueUrl(queueUrl).build)
      messagesList <- Observable.fromTask(
        SqsOp.receiveMessage.execute(messageRequest)(asyncClient).map(_.messages.asScala.toList))
      message <- Observable.fromIterable(messagesList)
    } yield message
  }

  def sink[Req <: SqsRequest, Resp <: SqsResponse](stopOnError: Boolean = false)(implicit sqsOp: SqsOp[Req, Resp]): Consumer[Req, Unit] =
    new SqsSink[Req, Req, Resp](in => in, sqsOp, asyncClient, stopOnError)

  def addPermission(queueUrl: String, actions: List[String], awsAccountIds: List[String],
                    label:String): Task[Unit] = {
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

  def deleteQueueByUrl(queueUrl: String): Task[Unit] = {
    val deleteRequest = DeleteQueueRequest.builder.queueUrl(queueUrl).build
    SqsOp.deleteQueue.execute(deleteRequest)(asyncClient).void
  }

  def createQueue(queueName: String,
                  tags: Map[String, String] = Map.empty,
                  attributes: Map[QueueAttributeName, String] = Map.empty): Task[String] = {
    val createQueueRequest = CreateQueueRequest.builder
      .queueName(queueName)
      .tags(tags.asJava)
      .attributes(attributes.asJava).build
    SqsOp.createQueue.execute(createQueueRequest)(asyncClient).map(_.queueUrl)
  }

  def deleteMessage(queueUrl: String, receiptHandle: String): Task[Unit] = {
    val deleteMessageRequest = DeleteMessageRequest.builder
      .queueUrl(queueUrl)
      .receiptHandle(receiptHandle).build
    SqsOp.deleteMessage.execute(deleteMessageRequest)(asyncClient).void
  }

  def deleteQueueByName(queueName: String): Task[Unit] = {
    for {
      queueUrl <- getQueueUrl(queueName)
      _ <- queueUrl.map(deleteQueueByUrl).getOrElse(Task.unit)
    } yield ()
  }

  def existsQueue(queueName: String): Task[Boolean] =
    getQueueUrl(queueName).map(_.isDefined)

  def getQueueUrl(queueName: String,
                  queueOwnerAWSAccountId: Option[String] = None): Task[Option[String]] = {
    val requestBuilder = GetQueueUrlRequest.builder.queueName(queueName)
    queueOwnerAWSAccountId.map(requestBuilder.queueOwnerAWSAccountId)
    SqsOp.getQueueUrl.execute(requestBuilder.build)(asyncClient)
      .map(r => Option(r.queueUrl))
      .onErrorHandleWith { ex =>
        if (ex.isInstanceOf[QueueDoesNotExistException]) {
          Task.now(Option.empty)
        } else {
          Task.raiseError(ex)
        }
      }
  }

  def listDeadLetterQueueUrls(queueUrl: String): Observable[String] = {
    val listRequest = ListDeadLetterSourceQueuesRequest.builder.queueUrl(queueUrl).build
    for {
      listResponse <- Observable.fromTask(SqsOp.listDeadLetter.execute(listRequest)(asyncClient))
      queueUrl <- Observable.fromIterable(listResponse.queueUrls.asScala)
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

  def listQueueTags(queueUrl: String): Task[Map[String,String]] = {
    val listRequest = ListQueueTagsRequest.builder.queueUrl(queueUrl).build
    SqsOp.listQueueTags.execute(listRequest)(asyncClient).map(_.tags.asScala.toMap)
  }

  //PurgeQueueInProgressException, QueueDeletedRecentlyException, QueueDoesNotExistException, QueueNameExistsException
  def purgeQueue(queueUrl: String): Task[Unit] = {
    val purgeQueue = PurgeQueueRequest.builder.queueUrl(queueUrl).build
    SqsOp.purgeQueue.execute(purgeQueue)(asyncClient).void
  }

  def sendMessage(queueMessage: QueueMessage,
                  queueUrl: String,
                  groupId: Option[String] = None,
                  delayDuration: Option[FiniteDuration] = None): Task[SendMessageResponse] = {
    val message = queueMessage.toMessageRequest(queueUrl, groupId, delayDuration)
    SqsOp.sendMessage.execute(message)(asyncClient)
  }

  def sendMessageSink(queueUrl: String,
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

  def setQueueAttribute(queueUrl: String, attributes: Map[QueueAttributeName, String]): Task[Unit] = {
    val setAttributesRequest = SetQueueAttributesRequest.builder.queueUrl(queueUrl).attributes(attributes.asJava).build
    SqsOp.setQueueAttributes.execute(setAttributesRequest)(asyncClient).void
  }

  def tagQueue(queueUrl: String, tags: Map[String, String]): Task[Unit] = {
    val tagQueueRequest = TagQueueRequest.builder.queueUrl(queueUrl).tags(tags.asJava).build
    SqsOp.tagQueue.execute(tagQueueRequest)(asyncClient).void
  }

  def untagQueue(queueUrl: String, tags: Map[String, String]): Task[Unit] = {
    val tagQueueRequest = TagQueueRequest.builder.queueUrl(queueUrl).tags(tags.asJava).build
    SqsOp.tagQueue.execute(tagQueueRequest)(asyncClient).void
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
