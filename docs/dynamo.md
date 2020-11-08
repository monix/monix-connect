---
id: dynamodb
title: AWS DynamoDB
---

## Introduction

_Amazon DynamoDB_ is a _key-value_ and document database that performs at any scale in a single-digit millisecond,
a key component for many platforms of the world's fastest growing enterprises that depend on it to support their mission-critical workloads.
   
The _DynamoDB_ api provides a large list of operations to _create_, _describe_, _delete_, _get_, _put, _batch_, _scan_, _list_ and more. 
All of them simply extend the same generic _request_ class [DynamoDbRequest](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/dynamodb/model/DynamoDbRequest.html),
and the same happens with the return types, they all implement [DynamoDbResponse](https://sdk.amazonaws.com/java/api/2.0.0/software/amazon/awssdk/services/dynamodb/model/DynamoDbResponse.html).  

The fact that all types implements from the same _Request_ and _Response_ type, makes it possible to create an abstraction layer on top of it for executing any operation in the same way.
_Credits:__ This design pattern is similar to the one used internally by _alpakka-dynamodb_, which we got inspired from. 

From there on, the connector provides three main methods: __single__, __transformer__ and __sink__ that implements the mentioned pattern and therefore allows to deal with 
 any dynamodb operation.  

## Dependency

Add the following dependency to get started:
```scala 
libraryDependencies += "io.monix" %% "monix-dynamodb" % "0.5.0"
```

## Async Client
 
 This connector uses the _underlying_ `S3AsyncClient` from the [java aws sdk](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/model/package-summary.html),
 it will allow us to authenticate and create a _non blocking_ channel between our application and the _AWS S3 service_. 
 
 There are different ways to create the connection, all available from the singleton object `monix.connect.s3.S3`. It is explained in more detail in the following sub-sections:
 
  This connector uses the _underlying_ [DynamoDbAsyncClient](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/dynamodb/DynamoDbAsyncClient.html),
 that allows us to authenticate and create a _non blocking_ channel between our application and the _AWS DynamoDB_. 
   

 ### From config
  
  The laziest and best way to create the _DynamoDb_ connection is from a configuration file.
  To do so, you'd just need to create an `application.conf` following the template [reference.conf](https://github.com/monix/monix-connect/tree/master/aws-auth/src/main/resources/reference.conf).
  If you don't create your own config file, it will default to the `reference.conf` mentioned just before, which uses `DefaultCredentialsProvider`.
    
    
  Below snippet shows an example of the configuration file to authenticate via `StaticCredentialsProvider` and region `EU-WEST-1`, as you can appreciate, the 
  _http client_ settings are commented out since they are optional, however they could have been specified too for a more grained configuration of the underlying `NettyNioAsyncHttpClient`.
    
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

This config file needs to be placed in the `resource` folder, 
therefore it will be automatically picked up from the method call `DynamoDb.fromConfig`, which will return a `cats.effect.Resource[Task, DynamoDb]`.
The [resource](https://typelevel.org/cats-effect/datatypes/resource.html) will be responsible to *acquire* and *release* the _DynamoDb connection_. 

The best way of using it is to make it transparent in your application and directly expect an instance of _S3_ in your methods and classes, which will be called from within the 
_usage_ of the _Resource_. See below code snippet to understand the concept:

```scala
 
import monix.connect.dynamodb.DynamoDb
import monix.connect.dynamodb.DynamoDbOp.Implicits._
import software.amazon.awssdk.services.dynamodb.model._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import monix.eval.Task

def runDynamoDbApp(dynamoDb: DynamoDb): Task[Array[Byte]] = {
  val tableName = "my-table"
  val createTableRequest: CreateTableRequest = ???
  val putItemRequest: PutItemRequest = ???
  for {
    _ <- dynamoDb.single(createTableRequest)
    _ <- dynamoDb.single(putItemRequest)
  } yield ()
}

// the connection gets created and released within the use method and the `DynamoDb`
// instance is directly passed to our application for an easier interoperability
val f = DynamoDb.fromConfig.use(runDynamoDbApp(_)).runToFuture
```  
    
 ### Create
 
 An alternative to using a config file is to pass the _AWS configurations_ by parameters.
 This is a safe implementation since the method, again, handles the _creation_ and _release_ of the connection with the _cats effect_ 
 `Resource` data type. The example below produce exactly the same result as previously using the _config_ file:
 
 ```scala
 import cats.effect.Resource
 import monix.connect.dynamodb.DynamoDb
 import monix.eval.Task
 import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
 import software.amazon.awssdk.regions.Region
 
 val s3AccessKey: String = "TESTKEY"
 val s3SecretKey: String = "TESTSECRET"
 val basicAWSCredentials = AwsBasicCredentials.create(s3AccessKey, s3SecretKey)
 val staticCredProvider = StaticCredentialsProvider.create(basicAWSCredentials)

 val dynamoDbResource: Resource[Task, DynamoDb] = DynamoDb.create(staticCredProvider, Region.AWS_GLOBAL)   
```

### Create Unsafe

Another different alternatively is to just pass an already created instance of a `software.amazon.awssdk.services.s3.S3AsyncClient`, which in that case, 
the return type would be directly `S3`, so there won't be no need to deal with `Resource`. 
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

### Single Operation 

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


It is also an _overloaded_ method that also accepts the number of retries after failure and delay between them, 
let's see in the following example how to use that to _query_ items from the table: 

```scala
import monix.eval.Task
import monix.connect.dynamodb.DynamoDb
import software.amazon.awssdk.services.dynamodb.model.{GetItemRequest, GetItemResponse}
import scala.concurrent.duration._

val getItemRequest: GetItemRequest = ???

import monix.connect.dynamodb.DynamoDbOp.Implicits.getItemOp
val t: Task[GetItemResponse] = 
  DynamoDb.fromConfig.use(_.single(getItemRequest, retries = 5, delayAfterFailure = 500.milliseconds))
```


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
