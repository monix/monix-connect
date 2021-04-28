package monix.connect.sqs

import monix.connect.sqs.domain.{QueueName, QueueUrl}
import monix.eval.Task
import monix.execution.internal.InternalApi
import monix.reactive.Observable.Transformer
import monix.reactive.Observable
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{AddPermissionRequest, ChangeMessageVisibilityRequest, CreateQueueRequest, DeleteMessageRequest, DeleteQueueRequest, GetQueueAttributesRequest, GetQueueUrlRequest, ListDeadLetterSourceQueuesRequest, ListQueueTagsRequest, ListQueuesRequest, PurgeQueueRequest, QueueAttributeName, QueueDoesNotExistException, SendMessageBatchRequest, SendMessageResponse, SetQueueAttributesRequest, SqsRequest, SqsResponse, TagQueueRequest, UntagQueueRequest}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

object SqsOperator {
  def create(implicit asyncClient: SqsAsyncClient) = new SqsOperator(asyncClient)
}
class SqsOperator private[sqs] (asyncClient: SqsAsyncClient) {


  //todo test
  def addPermission(queueUrl: QueueUrl, actions: List[String], awsAccountIds: List[String],
                    label: String): Task[Unit] = {
    val addPermissionReq = AddPermissionRequest.builder
      .queueUrl(queueUrl.url)
      .actions(actions: _*)
      .awsAccountIds(awsAccountIds: _*)
      .label(label).build
    SqsOp.addPermission.execute(addPermissionReq)(asyncClient).void
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

  def existsQueue(queueName: QueueName, queueOwnerAWSAccountId: Option[String] = None): Task[Boolean] =
    getQueueUrl(queueName, queueOwnerAWSAccountId)
      .map(Option(_))
      .onErrorHandleWith { ex =>
      if (ex.isInstanceOf[QueueDoesNotExistException]) {
        Task.now(Option.empty)
      } else {
        Task.raiseError(ex)
      }
    }.map(_.isDefined)

  def getQueueUrl(queueName: QueueName,
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

  //todo test
  //PurgeQueueInProgressException, QueueDeletedRecentlyException, QueueDoesNotExistException, QueueNameExistsException
  def purgeQueue(queueUrl: QueueUrl): Task[Unit] = {
    val purgeQueue = PurgeQueueRequest.builder.queueUrl(queueUrl.url).build
    SqsOp.purgeQueue.execute(purgeQueue)(asyncClient).void
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
