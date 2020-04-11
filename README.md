# Monix Connect
 
The Monix Connect is an open source initiative to implement stream integrations,
components and helpers for [Monix](https://monix.io/). The usage of these connectors would 
 reduce the boilerplate code, help the developer to easily integrate a different set of data sources 
 with the monix ecosystem, making the developers to reduce their learning curve on building projects that 
requires those components.

See below the list of available [connectors](#Connectors).  

## Connectors

1. [DynamoDB](#DynamoDB)
2. [Redis](#Redis)
3. [S3](#S3)

### DynamoDB
Amazon DynamoDB is a key-value and document datavase that performs at any scale in a single-digit millisecond.
In which of the world's fastest growing enterprises depend on it to support their mission-critical workloads.

The DynamoDB operations availavle are: __create table__, __delete table__, __put item__, __get item__, __batch get__ and __batch write__, in which 
seen under the java api prespective, all of them inherit from `DynamoDbRequest` and `DynamoDbResponse` respectively for requests and responses.

Therefore, `monix-dynamodb` makes possible to use a generic implementation of `Observable` transformer and consumer that handles with any DynamoDB request available in the `software.amazon.awssdk`. 

Transformer:
```
Observable
.fromIterable(dynamoDbRequests) 
.transform(DynamoDb.transofrmer()) //for each element transforms the request operations into its respective response 
//the resulted observable would be of type Observable[Task[DynamoDbRequest]]
```

Consumer: 

```
Observable
.fromIterable(dynamoDbRequests)
.consumeWith(DynamoDb.consume()) //a safe and syncronous consumer that executes each dynamodb request passed  
//the materialized value would be Task[DynamoDBResponse]
```
### Redis

### S3
