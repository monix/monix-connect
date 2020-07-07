---
id: dynamodb
title: AWS DynamoDB
---

## Introduction

_Amazon DynamoDB_ is a _key-value_ and document database that performs at any scale in a single-digit millisecond,
a key component for many platforms of the world's fastest growing enterprises that depend on it to support their mission-critical workloads.
   
The _DynamoDB_ api provides a large list of operations (create, describe, delete, get, put, batch, scan, list and more...), all them are simply designed in request and response pattern, 
in which from the api prespective, they have to respectively implement [DynamoDbRequest](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/dynamodb/model/DynamoDbRequest.html) and [DynamoDbResponse](https://sdk.amazonaws.com/java/api/2.0.0/software/amazon/awssdk/services/dynamodb/model/DynamoDbResponse.html).  

The fact that all types implements either _Request_ or _Response_ type makes possible to create an abstraction layer on top of it for executing the received requests and retunrn a future value with the respective response.
 
From there one, this connector provides two pre built implementations of a monix __transformer__ and __consumer__ that implements the mentioned pattern and therefore allowing the user to use them for any 
 given dynamodb operation.  

## Dependency

Add the following dependency to get started:
```scala 
libraryDependencies += "io.monix" %% "monix-dynamodb" % "0.1.0"
```

## Getting started

 This connector has been built on top of the [DynamoDbAsyncClient](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/dynamodb/DynamoDbAsyncClient.html), a client that only exposes non blocking operations,
  and that will permit the application to be authenticated and create a channel with the _AWS DynamoDB_ service.
  Therefore, all api operations that this connector exposes does expect and `implicit` instance of the async client.

It is also required and additional import `monix.connect.dynamodb.DynamoDbOp.Implicits._` for bringing the _implicit_ conversions between the specific `DynamoDBRequest` to `DynamoDbOp`, but you don't have to worry about the second data type 
since it is an abstraction from the internal api that provides a generic extension to the execution's description of any request given:
 
 The next sections will not just show an example for creating a _single request_ but also for transforming and consuming an `Observable[DynamoDbRequest]`.

### Single Execution 

There are cases where we do only want to execute a single request, it is the simplest scenario:

```scala
import monix.connect.dynamodb.DynamoDbOp
import monix.connect.dynamodb.DynamoDbOp.Implicits._
import software.amazon.awssdk.services.dynamodb.model.{PutItemRequest, PutItemResponse}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient


//presumably you'll have your specific request and an implicit async client
val request: PutItemRequest = ???
implicit val client : DynamoDbAsyncClient = ???

val t: Task[PutItemResponse] = DynamoDbOp.create(request)
```

This method defers the execution of the operation to be run later on, also accepts as an argument the number of `retries` and a `delay` after failure that would give some time to recover before the next retry (by default is set to not retry).

```scala
import monix.connect.dynamodb.DynamoDbOp
import monix.connect.dynamodb.DynamoDbOp.Implicits._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{GetItemRequest, GetItemResponse}

import scala.concurrent.duration._

val request: GetItemRequest = ???
implicit val client : DynamoDbAsyncClient = ???

val t: Task[GetItemResponse] = DynamoDbOp.create(request, retries = 5, delayAfterFailure = 500.milliseconds)
```

_Notice_ that the avobe code shows an example with `GetItemRequest` and `PutItemRequest`, but the method `.create` does accept any subtype of `DynamoDbRequest`, meaning that you could for example use operations such like: _create_, _describe_, _list_ or _restore_ a tables as well as _delete, _update_, _put_ or _get_ items individually or as batches.  
In case you want to know more about what operations are available, you should refer to the [AWS SDK javadoc](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/dynamodb/model/package-summary.html). 

### Consumer 

A pre-built _Monix_ `Consumer[DynamoDbRequest, DynamoDbResponse]` that provides a safe implementation 
for executing any subtype of `DynamoDbRequest` and materializes to its respective response.
It does also provide with the flexibility to specifying the number of times to retrying failed operations and the delay between them, which by default is no retry.

An example of a stream that consumes and executes DynamoDb `PutItemRequest`s:
```scala
import monix.connect.dynamodb.DynamoDbOp.Implicits._
import monix.connect.dynamodb.DynamoDb
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest

//presumably you will have a stream of DynamoDb requests coming in. 
val putItemRequests = List[PutItemRequest] = ???
implicit val client : DynamoDbAsyncClient = ???

val ob = Task[Unit] = {
  Observable
    .fromIterable(putItemRequests)
    .consumeWith(DynamoDb.consumer()) //asynchronous consumer that executes incoming put item requests
} 
//it materialises to Unit
```

_Notice_ that as same as the `DynamoDbOp.create()` method used for _single requests_ shown the previous section, the `DynamoDb.consumer()` also accepts a number of _retries_ and _delay after a failure_ to be passed.

### Transformer

Finally, the connector also provides a _transformer_ for `Observable`  that describes the execution of 
 any type of _DynamoDb_ request, returning its respective response: `Observable[DynamoDbRequest] => Observable[Task[DynamoDbResponse]]`.

The below code shows an example of a stream that transforms incoming get requests into its subsequent responses:

```scala
import monix.connect.dynamodb.DynamoDbOp.Implicits._
import monix.connect.dynamodb.DynamoDb
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{GetItemRequest, GetItemResponse}

//presumably you will have a stream of dynamo db `GetItemRequest`, this is just an example on how to use it
val dynamoDbRequests = List[GetItemRequest] = ???
implicit val client : DynamoDbAsyncClient = ???

val ob = Observable[Task[GetItemResponse]] = {
  Observable
    .fromIterable(dynamoDbRequests) 
    .transform(DynamoDb.transofrmer()) //transforms each get request operation into its respective get response 
} 

```

The _transformer_ accepts also a number of _retries_ and _delay after a failure_ to be passed.

## Local testing

[Localstack](https://github.com/localstack/localstack) provides a fully functional local _AWS_ cloud stack that in this case
the user can use to develop and test locally application's integration with _DynamoDB_.

Add the following service description to your `docker-compose.yaml` file:

```yaml
dynamodb:
  image: localstack/localstack:latest
  ports:
    - '4569:4569'
  environment:
    - SERVICES=dynamodb
```

Run the following command to build, and start the _DynamoDb_ service:

```shell script
docker-compose -f docker-compose.yml up -d dynamodb
``` 

Check out that the service has started correctly.

Finally create the client to connect to the local dynamodb via `DynamoDbAsyncClient`:

```scala
import java.net.URI

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

val defaultAwsCredProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x"))
implicit val client: DynamoDbAsyncClient = {
  DynamoDbAsyncClient
    .builder()
    .credentialsProvider(defaultAwsCredProvider)
    .endpointOverride(new URI("http://localhost:4569"))
    .region(Region.AWS_GLOBAL)
    .build()
  }
``` 
You are now ready to run your application! 
_Notice_ that the above example defines the `client` as `implicit`, since it is how the api will expect it.
