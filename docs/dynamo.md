---
id: dynamodb
title: AWS DynamoDB
---

## Introduction

_Amazon DynamoDB_ is a _key-value_ and document database that performs at any scale in a single-digit millisecond,
a key component for many platforms of the world's fastest growing enterprises that depend on it to support their mission-critical workloads.
   
The _DynamoDB_ api provides a large list of operations to _create_, _describe_, _delete_, _get_, _put, _batch_, _scan_, _list_ and more. 
All of them simply extend the same generic superclass on its input and output, the _request_ class they will extend [DynamoDbRequest](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/dynamodb/model/DynamoDbRequest.html),
and for the _response_ from [DynamoDbResponse](https://sdk.amazonaws.com/java/api/2.0.0/software/amazon/awssdk/services/dynamodb/model/DynamoDbResponse.html).  

The fact that all request types implements from the same superclass, makes it possible to create an abstraction layer on top of it for executing any operation in the same way.

_Credits:_ This design pattern is similar to the one used internally by _alpakka-dynamodb_, which this connector got inspiration from. 

Therefore, the connector provides three generic methods __single__, __transformer__ and __sink__ that implements the mentioned pattern and therefore allows to deal with 
 any _dynamodb_ operation in different circumstances.  

## Dependency

Add the following dependency to get started:
```scala 
libraryDependencies += "io.monix" %% "monix-dynamodb" % "0.6.0"
```

## Async Client

  This connector uses the _underlying_ [DynamoDbAsyncClient](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/dynamodb/DynamoDbAsyncClient.html),
 that allows us to authenticate and create a _non blocking_ channel between our application and the _AWS DynamoDB_. 
 
 There are different ways to create the connection, all available from the singleton object `monix.connect.dynamodb.DynamoDb`. Explained in more detail in the following sub-sections:

 ### From config
  
  
  The laziest and recommended way to create the _S3_ connection is to do it from configuration file.
  To do so, you'd just need to create an `application.conf` following the template from [reference.conf](https://github.com/monix/monix-connect/tree/master/aws-auth/src/main/resources/reference.conf),
  which also represents the default config in case no additional is provided.
    
  Below snippet shows an example of the configuration file to authenticate via `StaticCredentialsProvider` and region `EU-WEST-1`, as you can appreciate the 
  _http client_ settings are commented out since they are optional, however they could have been specified too for a more fine grained configuration of the underlying `NettyNioAsyncHttpClient`.
    
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

This config file should be placed in the `resources` folder, therefore it will be automatically picked up from the method call `S3.fromConfig`, which will return a `cats.effect.Resource[Task, S3]`.
The [resource](https://typelevel.org/cats-effect/datatypes/resource.html) is responsible of the *creation* and *release* of the _S3 client_. 

We recommend using it transparently in your application, meaning that your methods and classes will directly expect an instance of _S3_, which will be called from within the 
_usage_ of the _Resource_. See below code snippet to understand the concept:

```scala
import monix.connect.dynamodb.DynamoDb
import monix.connect.dynamodb.DynamoDbOp.Implicits._
import software.amazon.awssdk.services.dynamodb.model._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import monix.eval.Task
import pureconfig.KebabCase

def runDynamoDbApp(dynamoDb: DynamoDb): Task[Array[Byte]] = {
  val tableName = "my-table"
  val createTableRequest: CreateTableRequest = ???
  val putItemRequest: PutItemRequest = ???
  for {
    _ <- dynamoDb.single(createTableRequest)
    _ <- dynamoDb.single(putItemRequest)
  } yield ()
}

// It allows to specify the [[pureconfig.NamingConvention]] 
// from its argument, by default it uses the [[KebabCase]].
val f = DynamoDb.fromConfig(KebabCase).use(runDynamoDbApp).runToFuture
// the connection gets created and released within the use method and the `DynamoDb`
// instance is directly passed to our application for an easier interoperability
```  

There is an alternative way to use `fromConfig` which is to load the config first and then passing it to
the method. The difference is that in this case we will be able to override a specific configuration
from the code, whereas before we were reading it and creating the client straight after.

```scala
 import monix.connect.aws.auth.MonixAwsConf
 import monix.connect.dynamodb.DynamoDb
 import monix.eval.Task
 import software.amazon.awssdk.services.s3.model.NoSuchKeyException
 import pureconfig.KebabCase
 import software.amazon.awssdk.regions.Region

 val f = MonixAwsConf.load(KebabCase).memoizeOnSuccess.flatMap{ awsConf =>
   val updatedAwsConf = awsConf.copy(region = Region.EU_CENTRAL_1)
   DynamoDb.fromConfig(updatedAwsConf).use(runDynamoDbApp)
 }.runToFuture
```

## Create

On the other hand, one can pass the _AWS configurations_ by parameters, and that is safe since the method also handles the _acquisition_ and _release_ of the connection with
`Resource`. The example below produce exactly the same result as previously using the _config_ file:
 
 ```scala
 import cats.effect.Resource
 import monix.connect.dynamodb.DynamoDb
 import monix.eval.Task
 import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
 import software.amazon.awssdk.regions.Region
 
 val accessKey: String = "TESTKEY"
 val secretKey: String = "TESTSECRET"
 val basicAWSCredentials = AwsBasicCredentials.create(accessKey, secretKey)
 val staticCredProvider = StaticCredentialsProvider.create(basicAWSCredentials)

 val dynamoDbResource: Resource[Task, DynamoDb] = DynamoDb.create(staticCredProvider, Region.AWS_GLOBAL)   
```

### Create Unsafe

Another different alternatively is to just pass an already created instance of a `software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient`, 
which in that case the return type would be directly `DynamoDb`, so there won't be no need to deal with `Resource`. 
As the same tittle suggests, this is not a pure way of creating an `S3` since it might be possible to pass a malformed 
instance by parameter or just one that was already released (closed). 

An example:
 
 ```scala
import monix.connect.dynamodb.DynamoDb
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.regions.Region

val asyncClient: DynamoDbAsyncClient = 
  DynamoDbAsyncClient
    .builder
    .credentialsProvider(DefaultCredentialsProvider.create())
    .region(Region.EU_CENTRAL_1)
    .build

val dynamoDb: DynamoDb = DynamoDb.createUnsafe(asyncClient)
```

Notice that `createUnsafe` an overloaded method that also has just another variant that excepts settings values:

 ```scala
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region
import monix.connect.dynamodb.DynamoDb

val bucket: String = "my-bucket" 
val dynamoDb: DynamoDb = DynamoDb.createUnsafe(DefaultCredentialsProvider.create(), Region.AF_SOUTH_1)
```

### Implicit operation instances

 As mentioned in previous sections, the connector uses generic implementations for all type of requests.
  Because of that, the generic methods require the `implicit` dynamodb operator to be in the scope of the call.
   All the list of _implicit_ __operations__ can be found under `monix.connect.dynamodb.DynamoDbOp.Implicits._`, 
   from there on, you can import the instances of the operations that you want to use, or in contrast, 
   by importing them all you'll be able to execute any `DynamoDbRequest`.
  The next sections will not just show an example for creating a _single request_ but also for _transforming_ and _consuming_ streams of DynamoDB requests.

## Single Operation 

There are cases where we do only want to execute a __single request__, 
see on below snippet an example on how to create a table:

```scala
import monix.eval.Task
import monix.connect.dynamodb.DynamoDb
import software.amazon.awssdk.services.dynamodb.model._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

// our dummy table will contain citizen's data
case class Citizen(citizenId: String, city: String, debt: Double)

val tableName = "citizens"

// it is partitioned by `citizenId` and sorted by `city`
val keySchema: List[KeySchemaElement] = {
  List(
    KeySchemaElement.builder.attributeName("citizenId").keyType(KeyType.HASH).build,
    KeySchemaElement.builder.attributeName("city").keyType(KeyType.RANGE).build
  )
}

val tableDefinition: List[AttributeDefinition] = {
  List(
    AttributeDefinition.builder.attributeName("citizenId").attributeType(ScalarAttributeType.S).build(),
    AttributeDefinition.builder.attributeName("city").attributeType(ScalarAttributeType.S).build()
  )
}

val createTableRequest =
CreateTableRequest
  .builder
  .tableName(tableName)
  .keySchema(keySchema: _*)
  .attributeDefinitions(tableDefinition: _*)
  .billingMode(BillingMode.PAY_PER_REQUEST)
  .build()

//here is where we actually define the execution of creating a table 
import monix.connect.dynamodb.DynamoDbOp.Implicits.createTableOp
val t: Task[PutItemResponse] = DynamoDb.fromConfig.use(_.single(createTableRequest))
```

_Notice_ that in this case the `single` signature was used to create the table, but actually it does also accept any subtype of `DynamoDbRequest` with its respective operation from `monix.connect.dynamodb.DynamoDbOp.Implicits`.


The signature also accepts the number of _retries_ after failure and the delay between them, 
let's see in the following example how to use that to _query_ items from the table: 

```scala
import monix.eval.Task
import monix.connect.dynamodb.DynamoDb
import monix.connect.dynamodb.DynamoDbOp.Implicits.getItemOp // required to execute a get operation 
import monix.connect.dynamodb.domain.RetryStrategy
import software.amazon.awssdk.services.dynamodb.model.{GetItemRequest, GetItemResponse}

import scala.concurrent.duration._

val getItemRequest: GetItemRequest = ???
val retryStrategy = RetryStrategy(retries = 5, backoffDelay = 600.milliseconds)

val t: Task[GetItemResponse] = 
  DynamoDb.fromConfig.use(_.single(getItemRequest, retryStrategy))
```

## Consumer 

A pre-built _Monix_ `Consumer[DynamoDbRequest, DynamoDbResponse]` that provides a safe implementation 
for executing any subtype of `DynamoDbRequest` and materializes to its respective response.
It does also allow to specifying the backoff strategy, which represents the number of times to retrying failed operations and the delay between them, being _no retry_ by default.

```scala
import monix.connect.dynamodb.DynamoDbOp.Implicits._
import monix.connect.dynamodb.DynamoDb
import monix.connect.dynamodb.domain.RetryStrategy
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import software.amazon.awssdk.services.dynamodb.model.{PutItemRequest, AttributeValue}

import scala.concurrent.duration._


val strAttr: String => AttributeValue = value => AttributeValue.builder.s(value).build
val numAttr: Int => AttributeValue = value => AttributeValue.builder().n(value.toString).build

import monix.connect.dynamodb.DynamoDbOp.Implicits.putItemOp
val dynamoDb: DynamoDb
def putItemRequest(tableName: String, citizen: Citizen): PutItemRequest =
  PutItemRequest
    .builder
    .tableName(tableName)
    .item(Map("citizenId" -> strAttr(citizen.citizenId), "city" -> strAttr(citizen.city), "age" -> numAttr(citizen.age)).asJava)
    .build

val citizen1 = Citizen("citizen1", "Rome", 52)
val citizen2 = Citizen("citizen2", "Rome", 43)
val putItemRequests: List[PutItemRequest] = List(citizen1, citizen2).map(putItemRequest("citizens-table", _))

val t = Observable
    .fromIterable(putItemRequests)
    .consumeWith(dynamoDb.sink())
```

## Transformer

Finally, the connector also provides a _transformer_ for `Observable`  that describes the execution of 
 any type of _DynamoDb_ request, returning its respective response: `Observable[DynamoDbRequest] => Observable[DynamoDbResponse]`.

The below code shows an example of a stream that transforms incoming get requests into its subsequent responses:

```scala
import monix.connect.dynamodb.DynamoDbOp.Implicits._
import monix.connect.dynamodb.DynamoDb
import monix.reactive.Observable
import monix.execution.Scheduler.Implicits.global
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{GetItemRequest, GetItemResponse}

import monix.connect.dynamodb.DynamoDbOp.Implicits.getItemOp

def getItemRequest(tableName: String, citizenId: String, city: String): GetItemRequest =
   GetItemRequest.builder
     .tableName(tableName)
     .key(Map("citizenId" -> strAttr(citizen.citizenId), "city" -> strAttr(citizen.city)).asJava)
     .attributesToGet("age")
     .build

val getItemRequests: List[GetItemRequest] = List("citizen1", "citizen2").map(getItemRequest("citizens", _, city = "Rome"))

val dynamoDb: DynamoDb
val ob: Observable[GetItemResponse] = {
  Observable
    .fromIterable(getItemRequests) 
    .transform(dynamoDb.transformer())
} 

```

The _transformer_ signature also accepts also the number of _retries_ and _delay after a failure_ to be passed.

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

Run the following command to run the _DynamoDb_ service:

```shell script
docker-compose -f docker-compose.yml up -d dynamodb
``` 

Check out that the service has started correctly.

Finally create the _dynamodb connection_:

```scala
import monix.connect.dynamodb.DynamoDb
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region

val staticCredentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x"))
val dynamoDb = DynamoDb.create(staticCredentialsProvider, Region.AWS_GLOBAL, "http://localhost:4569")
``` 

Alternatively, you can create the client from config file, in this case you'd need to placed under the _resources_ folder `src/test/resources/application.conf`:

 ```hocon
{
  monix-aws: {
    credentials {
      provider: "static"  
      access-key-id: "x"
      secret-access-key: "x"    
    }
    endpoint: "http://localhost:4569"
    region: "us-west-2"
  }
}
```

As seen in previous sections, to create the connection from config:

```scala
import monix.connect.dynamodb.DynamoDb

val dynamoDb = DynamoDb.fromConfig
``` 

You are now ready to run your application! 
