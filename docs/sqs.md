---
id: sqs
title: AWS SQS
---

## Introduction

_Amazon Simple Queue Service (SQS)_  is a managed message queue service offered by Amazon Web Services (AWS).
It provides an HTTP API over which applications can submit items into and read items out of a queue.
The queue itself is fully managed by AWS, which makes SQS an easy solution
for passing messages between different parts of software systems that run in the cloud.  

## Dependency

Add the following dependency to get started:
```scala 
libraryDependencies += "io.monix" %% "monix-sqs" % "0.2.0"
```

## Getting started

 This connector has been built on top of the [SqsAsyncClient](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/sqs/SqsAsyncClient.html) since it only exposes non blocking operations,
  that will permit the application to be authenticated and create an channel with AWS Sqs service.

The `SqsAsyncClient` needs to be defined as `implicit` as it will be required by the _consumer_ and _tranformer_ implementations. 

It is also required and additional import `monix.connect.sqs.SqsOp._` for bringing the implicit conversions between the specific `SqsRequest` to `SqsOp`, you don't have to worry about the second data type 
, it is an abstraction that provides with the functionality to execute each request with its own operation:
 
See below an example for transforming and consuming Sqs operations with monix.

### Transformer

//this is an example of a stream that transforms and executes Sqs `ListQueuesRequest`:


```scala
import monix.connect.sqs.SqsOp._
import monix.connect.sqs.Sqs._
import software.amazon.awssdk.services.sqs.model.{ListQueuesRequest, ListQueuesResponse}

//presumably you will have a stream of sqs `ListQueuesRequest` requests coming in this case of type 
val sqsRequests = List[ListQueuesRequest] = ???

val ob = Observable[Task[ListQueuesResponse]] = {
  Observable
    .fromIterable(sqsRequests) 
    .transform(Sqs.transofrmer()) // transforms each get request operation into its respective get response 
} 
//the resulted observable would be of type Observable[Task[ListQueuesResponse]]
```

### Consumer 

An example of a stream that consumes and executes Sqs `CreateQueueRequest`s:
```scala
import monix.connect.sqs.SqsOp._
import software.amazon.awssdk.services.sqs.model.{CreateQueueRequest, CreateQueueResponse}

//presumably you will have a stream of sqs `CreateQueueRequest` coming in.  
val sqsRequests = List[CreateQueueRequest] = ???

val ob = Task[CreateQueueResponse] = {
  Observable
    .fromIterable(sqsRequests)
    .consumeWith(Sqs.consumer()) // asynchronous consumer that executes put item requests
} 
//the materialized value would be of type Task[CreateQueueResponse]
```

Note that both transformers and consumer builder are generic implementations for any `SqsRequest`, so you don't need
to explicitly specify its input and output types. 

## Local testing

[Localstack](https://github.com/localstack/localstack) provides a fully functional local AWS cloud stack that in this case
the user can use to develop and test locally and offline the integration of the application with SQS.

Add the following service description to your `docker-compose.yaml` file:

```yaml
sqs:
  image: localstack/localstack:latest
  ports:
    - '4576:4576'
  environment:
    - SERVICES=sqs
```

Run the following command to build, and start the sqs service:

```shell script
docker-compose -f docker-compose.yml up -d sqs
``` 

Check out that the service has started correctly.

Finally create the client to connect to the local sqs via `SqsAsyncClient`:

```scala
val defaultAwsCredProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x"))
implicit val client: SqsAsyncClient = {
  SqsAsyncClient
    .builder()
    .credentialsProvider(defaultAwsCredProvider)
    .endpointOverride(new URI("http://localhost:4576"))
    .region(Region.AWS_GLOBAL)
    .build()
  }
``` 
You are now ready to run your application! 
_Note that the above example defines the client as `implicit`, since it is how the api will expect it._
