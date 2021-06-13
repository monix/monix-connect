---
id: sqs
title: AWS SQS
---

## Introduction

The **Simple Queue Service** _(SQS)_  is a managed message queue service offered by **AWS**.
It provides an _HTTP API_ over which applications can produce and consume messages from different queues. 
A queue itself is fully managed by AWS, which makes _SQS_ an easy solution
for passing messages between different parts of software systems that run in the cloud, commonly
used as a message broker to communicate different systems, providing backpressure, availability, fault tolerance and more.

## Dependency

Add the following dependency to get started:
```scala 
libraryDependencies += "io.monix" %% "monix-sqs" % "0.6.0"
```

## Async Client

This connector uses the _underlying_ `SqsAsyncClient` from the [java aws sdk](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/sqs/package-summary.html),
allowing authenticating and interact through a _non-blocking_ HTTP connection, between our application and the _AWS SQS_.

This library offers different ways to create the connection, all available from the singleton object `monix.connect.sqs.Sqs` and explained in more detail in the following sub-sections.

### From config

The laziest and recommended way to create the _Sqs_ connection is to do it from configuration file.
It will return a cats effect `Resouce` with `Task`, which will take care of acquiring the aws client and releasing it after its usage.
To do so, you'd just need to create an `application.conf` following the template from [reference.conf](https://github.com/monix/monix-connect/tree/master/aws-auth/src/main/resources/reference.conf),
which also represents the default config in case no additional is provided.

Below snippet shows an example of the configuration file to authenticate via `StaticCredentialsProvider` and region `EU-WEST-1`, as you can appreciate the
_http client_ settings are commented out since they are optional, however they could have been specified too for a more fine-grained configuration of the underlying `NettyNioAsyncHttpClient`.

```hocon
{
  monix-aws: {
    credentials {
      // [anonymous, default, environment, instance, system, profile, static]
      provider: "static" 

      // optional - only required with static credentials
      static {
        access-key-id: "TESTKEY"      
        secret-access-key: "TESTSECRET"
        session-token: ""
      }
    }
  
    // [ap-south-1, us-gov-east-1, af-south-1, eu-west-2, ...]
    region: "eu-west-1"

    // optional
    #endpoint: ""

    // optional
    # http-client: {
    #   max-concurrency: 10
    #   max-pending-connection-acquires: 1000
    #   connection-acquisition-timeout: 2 minutes
    #   connection-time-to-live: 1 minute
    #   use-idle-connection-reaper: false
    #   read-timeout: 100 seconds
    #   write-timeout: 100 seconds
    # }
  }
}
```

This config file should be placed in the `resources` folder, therefore it will be automatically picked up from the method call `Sqs.fromConfig`, which will return a `cats.effect.Resource[Task, Sqs]`.
The [resource](https://typelevel.org/cats-effect/datatypes/resource.html) is responsible of the *creation* and *release* of the _Sqs client_.

**Try to reuse** the created **Sqs** client as much as possible in your application multiple times in your application. Otherwise, creating it
multiple times will waste precious resources... See below code snippet to understand the concept:

```scala
 import monix.connect.sqs.Sqs
 import monix.connect.sqs.domain.QueueName
 import monix.connect.sqs.producer.StandardMessage
 import monix.eval.Task
 import scalapb.descriptors.ScalaType.Message
 import software.amazon.awssdk.services.s3.model.NoSuchKeyException
 import scala.concurrent.duration._
 
 def runSqsApp(sqs: Sqs): Task[Array[Byte]] = {
   val queueName = QueueName("my-queue")
   for {
     queueUrl <- sqs.operator.createQueue(queueName)
     standardMessage = StandardMessage("sampleBody")
     _ <- sqs.producer.sendSingleMessage(queueUrl, standardMessage)
     receivedMessage <- sqs.consumer.receiveSingleManualDelete(queueUrl, waitTimeSeconds = 3.seconds)
   } yield receivedMessage
 }
 
  // the connection gets created and released within the use method and the `Sqs`
  // instance is directly passed and should be reused across our application
  val f = Sqs.fromConfig.use(runSqsApp).runToFuture
```  

### Create

An alternative to using a config file is to pass the _AWS configurations_ directly by parameters.
This is a safe implementation since the method also handles the _acquisition_ and _release_ of the connection within
`Resource`. The example below produce exactly the same result as previously using the _config_ file:

 ```scala
 import cats.effect.Resource
 import monix.connect.sqs.Sqs
 import monix.eval.Task
 import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
 import software.amazon.awssdk.regions.Region
 
 val accessKey: String = "TESTKEY"
 val secretKey: String = "TESTSECRET"
 val basicAWSCredentials = AwsBasicCredentials.create(accessKey, secretKey)
 val staticCredProvider = StaticCredentialsProvider.create(basicAWSCredentials)

 val sqs: Resource[Task, Sqs] = Sqs.create(staticCredProvider, Region.AWS_GLOBAL)   
```

### Create Unsafe

The last and less recommended alternatively to create the connection is to just pass an already created instance of a `software.amazon.awssdk.services.s3.S3AsyncClient`,
which in that case, the returns directly the `Sqs`, avoiding to dealing with `Resource`.
As the same title suggests, this is not a safe way of creating an `Sqs` since it puts on the developer the responsibility to correctly close the client,
making it less idiomatic and prone to errors like eagerly closing the connection or on the other hand, never releasing it, thus waisting resources.

An example:

 ```scala
import monix.connect.sqs.Sqs
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.regions.Region

val sqsAsyncClient: SqsAsyncClient = SqsAsyncClient
  .builder()
  .credentialsProvider(DefaultCredentialsProvider.create())
  .region(Region.EU_CENTRAL_1)
  .build()

val sqs: Sqs = Sqs.createUnsafe(sqsAsyncClient)
```

Notice that `createUnsafe` is an overloaded method that also accepts to pass the settings values separately:

 ```scala
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region
import monix.connect.sqs.Sqs

val sqs: Sqs = Sqs.createUnsafe(DefaultCredentialsProvider.create(), Region.AF_SOUTH_1)
```

## Core Components

Once the connection is created, we are ready to start sending and receiving messages to sqs queues.
Notice that the `Sqs` instance we created previously is a `case class` conformed of three basic components `Operator`, `Consumer`, `Producer`.

## Operator

This component aggregates the utility operations in the `SQS` domain, like creating and deleting and listing queues,
get the url out of the queue name, creating tags, permissions and so on.

### Create queue

#### Standard

Creating a standard queue only requires the queue name and returns its respective generated url, which will be used later
from other operations to refer to this queue, instead of by using the name. You can alternatively pass some queue attributes
to that queue like the `delaySeconds`, `visibilityTimeout`, etc. Otherwise, it will use their default values.

```scala
import monix.connect.sqs.{Sqs, SqsOperator}
import monix.connect.sqs.domain.QueueName

val sqs: Sqs = Sqs.fromConfig.use{ case Sqs(operator: SqsOperator, _, _) =>
  val queueName = QueueName("my-standard-queue")
  operator.createQueue(queueName)
}
```

#### Fifo

On the other hand, if the application or business logic requires to use a FIFO queue, it would require
the `FIFO_QUEUE` queue attribute to be `true` and the queue name ending with `.fifo` .
```scala
import monix.connect.sqs.{Sqs, SqsOperator}
import software.amazon.awssdk.services.sqs.model.QueueAttributeName
import monix.connect.sqs.domain.QueueName

Sqs.fromConfig.use { case Sqs(operator: SqsOperator, _, _) =>
  val fifoQueueName = QueueName("my-fifo-queue.fifo") //important that it ends with fifo 
  val queueAttributes = Map(QueueAttributeName.FIFO_QUEUE -> "true")
  operator.createQueue(fifoQueueName, attributes = queueAttributes)
}
```

#### Get queue url

The queue url is required on all operations, including producing and consuming messages.
This, the action eventually returns `QueueUrl` given its `QueueName`, however, it will return a failed `task` if queue name does not exist.

```scala
import monix.connect.sqs.{Sqs, SqsOperator}
import monix.connect.sqs.domain.QueueName

Sqs.fromConfig.use { case Sqs(operator: SqsOperator, _, _) =>
  val queueName = QueueName("my-queue")
  // the queue must already exist, otherwise it will fail 
  queueUrl <- operator.getQueueUrl(queueName)
}.runToFuture
```

#### Delete queue

Permanently deletes the queue from sqs from the given `QueueName`.
```scala
import monix.connect.sqs.Sqs
import monix.connect.sqs.domain.QueueName

Sqs.fromConfig.use { sqs =>
  val queueName = QueueName("my-queue")
  for {
    queueUrl <- sqs.operator.getQueueUrl(queueName)
    _ <- sqs.operator.deleteQUeue(queueUrl)
  } yield ()
}.runToFuture
```

#### Purge queue

Use this operation if you don't want to delete a queue instead remove all of its messages.
The process might take up to 60 seconds regardless of the queue's size.

```scala
import monix.connect.sqs.Sqs
import monix.connect.sqs.domain.QueueName

Sqs.fromConfig.use { sqs =>
  val queueName = QueueName("my-queue")
  for {
    queueUrl <- sqs.operator.getQueueUrl(queueName)
    _ <- sqs.operator.purgeQueue(queueUrl)
  } yield ()
}.runToFuture
```

#### List queues

Emits the queue urls in the current region.
The list operation can be conditioned with a `queuePrefix`, which then would only return the queues whose name starts with the given string.
Additionally, the number of elements returned can be limited with the `maxResults` argument.

```scala
import monix.connect.sqs.Sqs
import monix.connect.sqs.domain.QueueUrl
import monix.reactive.Observable

Sqs.fromConfig.use { sqs =>
  val maxResults = 10 //lists at most 10 queues
  val queuePrefix = "some" //only list queues that starts with this prefix
  val queuesObs: Observable[QueueUrl] = sqs.operator.listQueueUrls(Some(queuePrefix), Some(maxResults))
  //business logic here
  queuesObs.countL
}.runToFuture
```

## Producer

This library makes it easier to the user by distinguishing the message abstractions
to be sent in two types, `FifoMessage` and `StandardMessage`, which will be used relatively with the destination queue type.

```scala
package monix.connect.sqs.producer
final case class FifoMessage(body: String,
                       groupId: String,
                       deduplicationId: Option[String] = Option.empty,
                       messageAttributes: Map[String, MessageAttribute] = Map.empty,
                       awsTraceHeader: Option[MessageAttribute])
  extends Message(body, groupId = Some(groupId), deduplicationId = deduplicationId, messageAttributes, awsTraceHeader)
```

````scala
package monix.connect.sqs.producer
final case class StandardMessage(
  body: String,
  messageAttributes: Map[String, MessageAttribute] = Map.empty,
  awsTraceHeader: Option[MessageAttribute] = None,
  delayDuration: Option[FiniteDuration] = None)
  extends Message(body, groupId = None, deduplicationId = None, messageAttributes, awsTraceHeader, delayDuration)
````

As you can appreciate in above snippets, the `groupId` and `deduplicationId` are unique for `FifoMessages` whereas the `StandardMessage` is
the only one that has `delayDuration`.

These slight differences are the reason why they are kept in different classes, otherwise it would be confusing to work directly with,
and you would have to procure not to create an invalid event to the sqs queue.

Obviously, each message must be used with its respective queue type. Meaning that when producing a new
message, we must use `FifoMessages` for _fifo_ queues and `StandardMessage` for _standard_ ones.

In continuation, let's see the different existing signatures to produce message to `Sqs` queues, all of them can be used with either `FifoMessage`
or `StandardMessage`, as the two of them extend the same class, `monix.connect.sqs.producer.Message`.

### Send single message

This is the most basic produce method operation, in which only one message will be sent to the queue.

```scala
import monix.connect.sqs.Sqs
import monix.connect.sqs.domain.QueueName
import monix.connect.sqs.producer.StandardMessage

Sqs.fromConfig.use { case Sqs(operator, producer, _) =>
  val queueName = QueueName("my-queue")
  for {
    queueUrl <- operator.getQueueUrl(queueName)
    message = StandardMessage("Dummy content")
    response <- producer.sendSingleMessage(message, queueUrl)
  } yield response
}.runToFuture
```

### Send parallel batch

This operation instead of single message, it expects a list that is split in groups of ten and sent in batches in parallel.

```scala
import monix.connect.sqs.Sqs
import monix.connect.sqs.domain.QueueName
import monix.connect.sqs.producer.StandardMessage

Sqs.fromConfig.use { case Sqs(operator, producer, _) =>
  val queueName = QueueName("my-queue")
  for {
    queueUrl <- operator.getQueueUrl(queueName)
    messages = List(StandardMessage("1"), StandardMessage("2"), StandardMessage("3"))
    response <- producer.sendParBatch(messages, queueUrl)
  } yield response
}.runToFuture
```

### Send sink

A basic sink (aka monix `Consumer`) that listens to incoming `Messages` and produces them one by one to the specified queue.

```scala
import monix.connect.sqs.{Sqs, SqsOperator}
import monix.connect.sqs.domain.QueueName
import monix.connect.sqs.producer.FifoMessage
import monix.reactive.Observable
import software.amazon.awssdk.services.sqs.model.QueueAttributeName

Sqs.fromConfig.use { case Sqs(operator, producer, _) =>
  val groupId = "group-id123"
  val queueName = QueueName("my-fifo-queue.fifo")
  val queueAttributes = Map(QueueAttributeName.FIFO_QUEUE -> "true", QueueAttributeName.CONTENT_BASED_DEDUPLICATION -> "true")
  for {
    queueUrl <- operator.createQueue(fifoQueueName, attributes = queueAttributes)
    messages = List(FifoMessage("1", groupId), FifoMessage("2", groupId))
    _ <- Observable.fromIterable(messages).consumeWith(producer.sendSink(queueUrl))
  } yield ()
}.runToFuture
```

### Send par batch sink

A more advanced and performant version than `sendBatch`, which listens to lists of `Messages` to be emitted
splitting them in groups of at most ten messages that are sent in batches in parallel.

```scala
import monix.connect.sqs.Sqs
import monix.connect.sqs.domain.QueueName
import monix.connect.sqs.producer.StandardMessage
import monix.reactive.Observable
import software.amazon.awssdk.services.sqs.model.QueueAttributeName

Sqs.fromConfig.use { case Sqs(operator, producer, _) =>
  val queueName = QueueName("my-queue")
  // each batch can be of any size
  // the library takes care of splitting them in groups of 10
  // as it is the maximum size allowed in sqs
  val batch1 = List(StandardMessage("1"), StandardMessage("2"))
  val batch2 = List(StandardMessage("3"), StandardMessage("4"))

  for {
    queueUrl <- operator.createQueue(fifoQueueName, attributes = queueAttributes)
    _ <- Observable.fromIterable(List(batch1, batch2)).consumeWith(producer.sendParBatchSink(queueUrl))
  } yield ()
}.runToFuture
```

## Consumer

Messages from sqs queues can be consumed as `DeletableMessage` or just as plain `ConsumedMessage`, 
more details about it are to be explained in the following sub sections.

### Deletable message
As the name implies, a deletable message provides the user with the ability to delete a messages from its source queue,
such action is normally performed once the message is considered to be processed. The choice for an application with
**at least once** messaging semantics.

```scala
import monix.connect.sqs.{Sqs, SqsOperator}
import monix.connect.sqs.domain.QueueName
import monix.connect.sqs.producer.DeletableMessage

def fakeDbInsert(deletableMessage: DeletableMessage): Task[Unit]

Sqs.fromConfig.use { case Sqs(operator, _, consumer) =>
  val queueName = QueueName("my-queue")
  val groupId = "groupId123"
  val deduplicationId = "deduplicationId123"
  for {
    queueUrl <- operator.getQueueUrl(queueName)
    message = FifoMessage(body = "my dummy content", groupId = groupId, deduplicationId = deduplicationId)
    _ <- consumer.receiveManualDelete(queueUrl) //: Observable[DeletableMessage]
      .doOnNext(fakeDbInsert)
      // only after the message is inserted in the db we delete the message from the queue 
      .mapEval(deletableMessage => deletableMessage.deleteFromQueue()) 
      .completedL
  } yield response
}.runToFuture
```

If you prefer to only perform a single receive request, there is also a method called `receiveSingleManualDelete`, which will return
a list of as many messages as specified in the `maxMessages` argument (max 10), wrapped in a `Task`.

### Auto deleted message

On the other hand there might be cases where it is preferred to implemented our application with **at most one** semantics,
for instance when it must not process the same message twice, or it's affordable to losing messages.

In these cases we could just use the auto deleted signatures, which will emit events that
are already deleted from the queue, so that the user will not have to worry about processing it multiple times nor handling its deletion.

There are two different signatures for consuming auto/already-deleted messages, the first one performs a single receive request and is called
`receiveSingleAutoDelete`, returning a list of at most 10 messages wrapped in `Task`.
Therefore, the streaming version of such is just called `receiveAutoDelete`, which returns an `Observable` that
keeps emitting plain `ConsumedMessage`s.

## Local testing

### Localstack
[Localstack](https://github.com/localstack/localstack) provides a fully functional local AWS cloud stack that in this case
the user can use to develop and test locally and offline the integration of the application with SQS.

Add the following service description to your `docker-compose.yaml` file:

```yaml
services:
  localstack:
    image: localstack/localstack:0.11.1
    ports:
      - '4576:4576'
  environment:
    - SERVICES=sqs #prevents to start the rest of the aws services
```

### Elasticmq

```yaml
services:
  elasticmq:
    image: softwaremill/elasticmq-native:latest
    ports:
      - '9324:9324'
```

Run the docker command to build, and start the sqs service:

```shell script
docker-compose -f docker-compose.yml up -d 
```

Check out that the service has started correctly.

Below snippet shows a sample of the settings used to locally `Sqs` using a `hocoon` config file, which should be placed
under your test resources folder `src/test/resources`.

 ```hocon
{
  monix-aws: {
    credentials {
      provider: "static"  
      access-key-id: "TESTKEY"
      secret-access-key: "TESTSECRET"    
    }
    endpoint: "http://localhost:9324"
    region: "us-west-2"
  }
}
```

Alternatively you can statically create connection by parameters:

```scala
import monix.connect.sqs.Sqs
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}

import java.net.URI

val endPoint: String = "http://localhost:9324"

val awsAccessKey: String = "TESTKEY" 
val awsSecretKey: String = "TESTSECRET" 

val basicAWSCredentials = AwsBasicCredentials.create(awsAccessKey, awsSecretKey)
val staticCredentialsProvider = StaticCredentialsProvider.create(basicAWSCredentials)

val sqs = Sqs.create(credentialsProvider = basicAWSCredentials, region = Region.EU_WEST_1, endpoint = Some(endPoint))
```

Now you are ready to have fun!




