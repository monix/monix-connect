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

import monix.connect.sqs.domain.{QueueName, QueueUrl}
import monix.eval.Task
import monix.execution.internal.InternalApi
import monix.reactive.Observable.Transformer
import monix.reactive.Observable
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{
  AddPermissionRequest,
  ChangeMessageVisibilityRequest,
  CreateQueueRequest,
  DeleteMessageRequest,
  DeleteQueueRequest,
  GetQueueAttributesRequest,
  GetQueueUrlRequest,
  ListDeadLetterSourceQueuesRequest,
  ListQueueTagsRequest,
  ListQueuesRequest,
  PurgeQueueRequest,
  QueueAttributeName,
  QueueDoesNotExistException,
  RemovePermissionRequest,
  SendMessageBatchRequest,
  SendMessageResponse,
  SetQueueAttributesRequest,
  SqsRequest,
  SqsResponse,
  TagQueueRequest,
  UntagQueueRequest
}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

object SqsOperator {
  def create(implicit asyncClient: SqsAsyncClient) = new SqsOperator(asyncClient)
}

class SqsOperator private[sqs] (asyncClient: SqsAsyncClient) {

  /** todo test
    * Adds a permission to a queue for a specific <a href="https://docs.aws.amazon.com/general/latest/gr/glos-chap.html#P">principal</a>.
    * This allows sharing access to the queue.
    * When you create a queue, you have full control access rights for the queue.
    * Only you, the owner of the queue, can grant or deny permissions to the queue.
    * For more information about these permissions,
    * see <a href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-writing-an-sqs-policy.html#write-messages-to-shared-queue">
    * Allow Developers to Write Messages to a Shared Queue</a>
    *
    * Add permission generates a default policy for you.
    * You can also use [[setQueueAttributes]] to upload your policy.
    * For more information, see
    * `Using Custom Policies with the Amazon SQS Access Policy Language`.
    *
    * Some actions take lists of parameters.
    * These lists are specified using the <code>param.n</code> notation.
    * Values of `n` are integers starting from 1.
    * For example, a parameter list with two elements looks like this:
    * `&amp;AttributeName.1=first`
    * `&amp;AttributeName.2=second`
    *
    * @param queueUrl      the url of the queue, obtained from [[getQueueUrl]].
    * @param actions       The action the client wants to allow for the specified principal.
    *                      Valid values: the name of any action or *.
    *                      I.e: SendMessage, DeleteMessage, ChangeMessageVisibility...
    *                      See <a href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-overview-of-managing-access.html">
    *                      Overview of Managing Access Permissions</a>
    * @param awsAccountIds The AWS account number of the
    *                      <a href="https://docs.aws.amazon.com/general/latest/gr/glos-chap.html#P">principal</a> who is given
    *                      permission. The principal must have an AWS account, but does not need to be signed up for Amazon SQS.
    *                      For information about locating the AWS account identification, see <a href=
    *                      "https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-making-api-requests.html#sqs-api-request-authentication">
    *                      Your AWS Identifiers</a>
    * @param label         The unique identification of the permission
    *                      to be set (for example, AliceSendMessage).
    *                      Maximum 80 characters. Allowed characters include
    *                      alphanumeric characters, hyphens (-), and underscores (_).
    *
    * @see also the aws sqs api docs, where the above come from ;)
    *
    */
  def addPermission(
    queueUrl: QueueUrl,
    actions: List[String],
    awsAccountIds: List[String],
    label: String): Task[Unit] = {
    val addPermissionReq = AddPermissionRequest.builder
      .queueUrl(queueUrl.url)
      .actions(actions: _*)
      .awsAccountIds(awsAccountIds: _*)
      .label(label)
      .build
    SqsOp.addPermission.execute(addPermissionReq)(asyncClient).void
  }

  /**
    * Removes a permissions in the queue policy that matches the specified label.
    *
    * @param queueUrl the url of the queue. It can be obtained from [[getQueueUrl]].
    * @param label The unique identification of the permission to be removed.
    *              Maximum 80 characters. Allowed characters include alphanumeric
    *              characters, hyphens (-), and underscores (_).
    */
  def removePermission(queueUrl: QueueUrl, label: String): Task[Unit] = {
    val removePermissionRequest = RemovePermissionRequest.builder
      .queueUrl(queueUrl.url)
      .label(label)
      .build
    SqsOp.removePermission.execute(removePermissionRequest)(asyncClient).void
  }

  /**
    * Deletes a queue.
    *
    * @param queueUrl the url of the queue to be deleted.
    */
  def deleteQueue(queueUrl: QueueUrl): Task[Unit] = {
    val deleteRequest = DeleteQueueRequest.builder.queueUrl(queueUrl.url).build
    SqsOp.deleteQueue.execute(deleteRequest)(asyncClient).void
  }

  /**
    * Creates a new standard or FIFO queue.
    * You can pass one or more attributes in the request.
    *
    * If you don't specify the FifoQueue attribute, Amazon SQS creates a standard queue.
    *
    * {{{
    *   import software.amazon.awssdk.services.sqs.model.QueueAttributeName
    *   // it would be the same as not specifying at all the `FIFO_QUEUE` attribute.
    *   val standardAttributes = Map(QueueAttributeName.FIFO_QUEUE -> "false")
    *   val fifoAttributes = Map(QueueAttributeName.FIFO_QUEUE -> "true")
    * }}}
    *
    * It is not possible to change the queue type after it is created.
    * You must either create a new FIFO queue for your application or
    * delete your existing standard queue and recreate it as a FIFO queue.
    * For more information, see Moving From a Standard Queue to a
    * FIFO Queue in the Amazon Simple Queue Service Developer Guide.
    *
    * The queue name has to adhere to the limits related to queues
    * and it is unique within the scope of the queue.
    * After a queue is created, it is necessary to wait at least one second
    * after the queue is created to be able to use the queue.
    *
    * @param queueName the queueName to be created.
    *                  fifo queues have to be suffixed with `.fifo`
    * @param tags map of cost allocation tags to the specified to the Amazon SQS queue.
    *             For an overview, see Tagging Your Amazon SQS Queues
    *             in the Amazon Simple Queue Service Developer Guide.
    *             Tags are case-sensitive.
    *             A new tag with a key identical to that of an existing tag overwrites the existing tag.
    *             For a full list of tag restrictions, see Limits Related to Queues in the Developer Guide.
    *             It requires sqs:CreateQueue and sqs:TagQueue permissions.
    * @param attributes A map of [[QueueAttributeName]]s with their corresponding values.
    *                   The `FIFO_QUEUE` and `CONTENT_BASED_DEDUPLICATION` attributes only apply to Fifo queues.
    *                   See the Amazon Single Queue Service Developer Guide for more details.
    *
    */
  def createQueue(
    queueName: QueueName,
    tags: Map[String, String] = Map.empty,
    attributes: Map[QueueAttributeName, String] = Map.empty): Task[QueueUrl] = {
    val createQueueRequest = CreateQueueRequest.builder
      .queueName(queueName.name)
      .tags(tags.asJava)
      .attributes(attributes.asJava)
      .build
    SqsOp.createQueue
      .execute(createQueueRequest)(asyncClient)
      .map(response => QueueUrl(response.queueUrl))
  }

  /**
    * Checks whether the queue exists or not.
    *
    * It might be useful to verify before creating or trying to get the url.
    *
    * @param queueName the name of the queue.
    * @param queueOwnerAWSAccountId the AWS account ID of the account that created the queue.
    */
  def existsQueue(queueName: QueueName, queueOwnerAWSAccountId: Option[String] = None): Task[Boolean] =
    getQueueUrl(queueName, queueOwnerAWSAccountId)
      .map(Option(_))
      .onErrorHandleWith { ex =>
        if (ex.isInstanceOf[QueueDoesNotExistException]) {
          Task.now(Option.empty)
        } else {
          Task.raiseError(ex)
        }
      }
      .map(_.isDefined)

  /**
    * Get the [[QueueUrl]] of an existing Amazon SQS queue.
    * To access a queue that belongs to another AWS account,
    * use the QueueOwnerAWSAccountId parameter to specify
    * the account ID of the queue's owner.
    * The queue's owner must grant you permission to access the queue.
    *
    * @param queueName the name of the queue.
    * @param queueOwnerAWSAccountId the AWS account ID of the account that created the queue.
    */
  def getQueueUrl(queueName: QueueName, queueOwnerAWSAccountId: Option[String] = None): Task[QueueUrl] = {
    val requestBuilder = GetQueueUrlRequest.builder.queueName(queueName.name)
    queueOwnerAWSAccountId.map(requestBuilder.queueOwnerAWSAccountId)
    SqsOp.getQueueUrl.execute(requestBuilder.build)(asyncClient).map(r => QueueUrl(r.queueUrl))
  }

  /**
    * Gets attributes for the specified queue.
    * To determine whether a queue is FIFO , you can check whether QueueName ends with the .fifo suffix.
    * @param queueUrl
    * @return
    */
  def getQueueAttributes(queueUrl: QueueUrl): Task[Map[QueueAttributeName, String]] = {
    val getAttributesRequest =
      GetQueueAttributesRequest.builder.queueUrl(queueUrl.url).attributeNames(QueueAttributeName.ALL).build
    SqsOp.getQueueAttributes
      .execute(getAttributesRequest)(asyncClient)
      .map(_.attributes.asScala.toMap)
  }

  //todo test
  def isFifoQueue(queueName: QueueName): Task[Boolean] = {
    if (queueName.name.endsWith(".fifo"))
      existsQueue(queueName)
    else Task.now(false)
  }

  //todo test
  def listDeadLetterQueueUrls(queueUrl: QueueUrl): Observable[QueueUrl] = {
    val listRequest = ListDeadLetterSourceQueuesRequest.builder.queueUrl(queueUrl.url).build
    for {
      listResponse <- Observable.fromReactivePublisher {
        asyncClient.listDeadLetterSourceQueuesPaginator(listRequest)
      }
      queueUrl <- Observable.fromIterable(listResponse.queueUrls.asScala)
    } yield QueueUrl(queueUrl)
  }

  /**
    * Lists all the existing queue urls from your current region.
    *
    * @param queuePrefix optional parameter that adds a request filter
    *                    to only return [[QueueUrl]]s that begins with
    *                    the specified value.
    *
    * @param maxResults specify the maximum number of queueUrls to be listed.
    * @return a stream of [[QueueUrl]]s from the current region.
    */
  def listQueueUrls(queuePrefix: Option[String] = None, maxResults: Int = 1000): Observable[QueueUrl] = {
    val listRequestBuilder = ListQueuesRequest.builder
      .maxResults(maxResults)
    queuePrefix.map(listRequestBuilder.queueNamePrefix)
    for {
      listResponse <- Observable.fromReactivePublisher {
        asyncClient.listQueuesPaginator(listRequestBuilder.build)
      }
      queueUrl <- Observable.fromIterable(listResponse.queueUrls.asScala)
    } yield QueueUrl(queueUrl)
  }

  /**
    * List all cost allocation tags added to the specified Amazon SQS queue.
    *
    * @see <a href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-queue-tags.html">
    *        Tagging Your Amazon SQS Queues</a>.
    *
    * Cross-account permissions don't apply to this action.
    * For more information, see the Grant cross-account
    * permissions to a role and a user name in the Amazon Simple
    * Queue Service Developer Guide.
    *
    * @param queueUrl the url of the queue, obtained from [[getQueueUrl]].
    */
  def listQueueTags(queueUrl: QueueUrl): Task[Map[String, String]] = {
    val listRequest = ListQueueTagsRequest.builder.queueUrl(queueUrl.url).build
    SqsOp.listQueueTags.execute(listRequest)(asyncClient).map(_.tags.asScala.toMap)
  }

  //todo test
  //PurgeQueueInProgressException, QueueDeletedRecentlyException, QueueDoesNotExistException, QueueNameExistsException
  /**
    * Deletes the messages in a queue specified by the [[QueueUrl]].
    *
    * After performing a purge action, it is not possible
    * to retrieve any messages from the queue.
    * The message deletion process takes up to 60 seconds.
    * We recommend waiting for 60 seconds regardless of your
    * queue's size.
    *
    * Messages sent to the queue after this call might
    * be deleted while the queue is being purged.
    *
    * It can return a failed task due with the following exceptions:
    * PurgeQueueInProgressException,QueueDeletedRecentlyException, QueueDoesNotExistException.
    *
    * @param queueUrl the url of the queue to be purged,
    *                 can be obtained from [[getQueueUrl]].
    */
  def purgeQueue(queueUrl: QueueUrl): Task[Unit] = {
    val purgeQueue = PurgeQueueRequest.builder.queueUrl(queueUrl.url).build
    SqsOp.purgeQueue.execute(purgeQueue)(asyncClient).void
  }

  /**
    * Sets the value of one or more queue attributes.
    * When you change a queue's attributes, the change can take up to 60 seconds
    * for most of the attributes to propagate throughout the Amazon SQS system.
    * Changes made to the MessageRetentionPeriod attribute can take up to 15 minutes.
    *
    * @see https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SetQueueAttributes.html
    * @param queueUrl the url of the queue whose attributes are set,
    *                 can be obtained from [[getQueueUrl]].
    * @param attributes the map of attributes to be set.
    */
  def setQueueAttributes(queueUrl: QueueUrl, attributes: Map[QueueAttributeName, String]): Task[Unit] = {
    val setAttributesRequest =
      SetQueueAttributesRequest.builder.queueUrl(queueUrl.url).attributes(attributes.asJava).build
    SqsOp.setQueueAttributes.execute(setAttributesRequest)(asyncClient).void
  }

  /**
    * Add cost allocation tags to the specified Amazon SQS queue.
    *
    * @see <a href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-queue-tags.html">
    *        Tagging Your Amazon SQS Queues </a>.
    * @param queueUrl the url of the queue whose tags are added to,
    *                 it can be obtained from [[getQueueUrl]].
    * @param tags list of tags to be added to the specified queue.
    */
  def tagQueue(queueUrl: QueueUrl, tags: Map[String, String]): Task[Unit] = {
    val tagQueueRequest = TagQueueRequest.builder.queueUrl(queueUrl.url).tags(tags.asJava).build
    SqsOp.tagQueue.execute(tagQueueRequest)(asyncClient).void
  }

  /**
    * Remove the specified tags from an Amazon SQS queue.
    *
    * @see <a href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-queue-tags.html">
    *        Tagging Your Amazon SQS Queues </a>.
    * @param queueUrl the url of the queue whose tags are added to,
    *                 it can be obtained from [[getQueueUrl]].
    * @param tagKeys list of tags to be removed from the specified queue.
    */
  def untagQueue(queueUrl: QueueUrl, tagKeys: List[String]): Task[Unit] = {
    val untagQueueRequest = UntagQueueRequest.builder.queueUrl(queueUrl.url).tagKeys(tagKeys.asJava).build
    SqsOp.untagQueue.execute(untagQueueRequest)(asyncClient).void
  }

  def transformer[In <: SqsRequest, Out <: SqsResponse](
    implicit
    sqsOp: SqsOp[In, Out]): Transformer[In, Out] = { inObservable: Observable[In] =>
    inObservable.mapEval(in => sqsOp.execute(in)(asyncClient))
  }

}
