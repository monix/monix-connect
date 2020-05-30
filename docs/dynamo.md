---
id: dynamodb
title: AWS DynamoDB
---

## Introduction

_Amazon DynamoDB_ is a key-value and document database that performs at any scale in a single-digit millisecond,
a key component for many platforms of the world's fastest growing enterprises that depend on it to support their mission-critical workloads.
   
The DynamoDB api provides a large list of operations (create, describe, delete, get, put, batch, scan, list and more...), all them are simply designed in request and response pattern, 
in which from the api prespective, they have to respectively implement [DynamoDbRequest](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/dynamodb/model/DynamoDbRequest.html) and [DynamoDbResponse](https://sdk.amazonaws.com/java/api/2.0.0/software/amazon/awssdk/services/dynamodb/model/DynamoDbResponse.html).  

The fact that all types implements either Request or Response type makes possible to create on top of that an abstraction layer that executes the received requests and retunrn a future value with the respective response.
 
From there one, this connector provides two pre built implementations of a monix __transformer__ and __consumer__ that implements the mentioned pattern and therefore allowing the user to use them for any 
 given dynamodb operation.  

## Dependency

Add the following dependency to get started:
```scala 
libraryDependencies += "io.monix" %% "monix-dynamodb" % "0.1.0"
```

## Getting started

 This connector has been built on top of the [DynamoDbAsyncClient](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/dynamodb/DynamoDbAsyncClient.html) since it only exposes non blocking operations,
  that will permit the application to be authenticated and create an channel with AWS DynamoDB service.

The `DynamoDbAsyncClient` needs to be defined as `implicit` as it will be required by the _consumer_ and _tranformer_ implementations. 

It is also required and additional import for bringing the implicit conversions between DynamoDB requests and `DynamoDbOp`, the upper abstraction layer will allow to exacute them all (no need to worry about that):
 
  ```scala
import monix.connect.dynamodb.DynamoDbOp._
```
 
See below an example for transforming and consuming DynamoDb operations with monix.

###_Transformer:_
```scala
//this is an example of a stream that transforms and executes DynamoDb `GetItemRequests`:
val dynamoDbRequests = List[GetItemRequest] = ???

val ob = Observable[Task[GetItemResponse]] = {
  Observable
    .fromIterable(dynamoDbRequests) 
    .transform(DynamoDb.transofrmer())
} //for each element transforms the get request operations into its respective get response 
//the resulted observable would be of type Observable[Task[GetItemResponse]]
```

###_Consumer:_ 
```scala
//this is an example of a stream that consumes and executes DynamoDb `PutItemRequest`:
val putItemRequests = List[PutItemRequests] = ???

Observable
.fromIterable(dynamoDbRequests)
.consumeWith(DynamoDb.consumer()) //a safe and syncronous consumer that executes dynamodb requests  
//the materialized value would be of type Task[PutItemResponse]
```

Note that both transformers and consumer builder are generic implementations for any `DynamoDbRequest`, so you don't need
to explicitly specify its input and output types. 

## Local testing

[Localstack](https://github.com/localstack/localstack) provides a fully functional local AWS cloud stack that in this case
the user can use to develop and test locally and offline the integration of the application with DynamoDB.

Add the following service description to your `docker-compose.yaml` file:

```yaml
dynamodb:
  image: localstack/localstack:latest
  ports:
    - '4569:4569'
  environment:
    - SERVICES=dynamodb
```

Run the following command to build, and start the dynamodb service:

```shell script
docker-compose -f docker-compose.yml up -d dynamodb
``` 

Check out that the service has started correctly.

Finally create the client to connect to the local dynamodb via `DynamoDbAsyncClient`:

```scala
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
_Note that the above example defines the client as `implicit`, since it is how the api will expect it._
