---
id: mongodb 
title: MongoDB
---

## Introduction

_MongoDB_ is a _document database_, in which the data is stored in _JSON-like documents_ differing to the traditional _
row/column_ model. It has a rich and expressive query language that allows you to filter and sort by any field with
support for aggregations and other modern use-cases. The _Monix MongoDB_ connector offers a _reactive_, _non blocking_
and _resource safe_ api, which relies in the underlying
the [MongoDB Java Reactive Streams](https://docs.mongodb.com/drivers/reactive-streams) driver. The library core data type 
to interoperate with collections is `CollectionOperator[Doc]`, which is composed by the following four main pillars:
- __Database:__ Used to manage and dealing with _mongo databases_ and _collections_.
- __Single:__ It exposes single operations to delete, insert, replace and update _collections_.
- __Sink:__ Implements the same operations as the _Operation_ api, but it in a streaming fashion.
- __Source:__ Used to fetch data from the collections with _aggregate_, _count_, _distinct_ and _find_.

Each of these components is explained in detail in the following sub-sections, 
but before we will see how configure and connect to the database server.

## Dependency

Add the following dependency to get started:

```scala
libraryDependencies += "io.monix" %% "monix-mongodb" % "0.6.0"
```

## Collection Reference

Before creating the connection, we would need to have a **reference** to the **collection** that we want to interoperate with
and use for storing and reading _documents_.

Such _reference_ is identified by the _database_ and _collection_ names, and represented in code by
the `CollectionRef` _sealed trait_, which is inherited by `CollectionCodecRef` and `CollectionDocRef`.

### CollectionCodecRef

The most practical way of representing documents in _Scala_ code is by using a _case class_, in which then they
would be auto-derived as `CodecProvider` using implicit conversions
from `import org.mongodb.scala.bson.codecs.Macros._`.

For didactic purposes we have defined a hypothetical `Employee` class with its `employees` _collection_, which
that will be used to show how to create a collection reference with our custom _Codec_ that will later be used to create the `MongoConnection`.

```scala
import org.mongodb.scala.bson.codecs.Macros.createCodecProvider
import monix.connect.mongodb.client.CollectionCodecRef
import monix.connect.mongodb.client.CollectionRef

case class Employee(name: String, age: Int, city: String, hobbies: List[String] = List.empty)

val employee = Employee("Bob", 29, "Barcelona")
val employeesCol: CollectionRef[Employee] =
  CollectionCodecRef("myDb", "employees", classOf[Employee], createCodecProvider[Employee]())
```

### CollectionDocumentRef

On the other hand, one could also use `org.bson.Document` as a generic and flexible type for representing a _document_.
In that case `CollectionDocumentRef` will be used, which would only require the _db_ and _collection_ name to be
created:

```scala
import monix.connect.mongodb.client.{CollectionDocumentRef, CollectionRef}
import org.bson.Document

val sampleDocument: Document = Document.parse("""{"film_name":"Jumanji", "year": 1995 }""")
val documentCol: CollectionRef[Document] = CollectionDocumentRef("myDb", "employees")
```

## Connection

Once we have created _collection reference_, we will pass it to the method that creates the _connection_
which also requires either a **mongo** **connectionString** or **client settings**. You can find below an example of
connecting to a _standalone MongoDB server_, but being also configurable for _Replica Set_, and _Sharded Cluster_  with
different options such _TLS/SSL_, etc. For more details, please refer to
the [official documentation](https://mongodb.github.io/mongo-java-driver/4.1/driver-reactive/tutorials/connect-to-mongodb/)
.

### Connection String

See in below code snippet how to create the connection with **connection string**:

```scala
import cats.effect.Resource
import monix.connect.mongodb.client.{CollectionCodecRef, MongoConnection}
import org.mongodb.scala.bson.codecs.Macros.createCodecProvider
import com.mongodb.client.model.Filters

val employeesCol = CollectionCodecRef("myDb", "employees", classOf[Employee], createCodecProvider[Employee]())
val connectionStr = "mongodb://localhost:27017"
val connection = MongoConnection.create1(connectionStr, employeesCol)
```

### Client Settings

On the other hand, you could also build the configuration in a typed way with `MongoClientSettings`, see an example in
below snippet:

```scala
import monix.connect.mongodb.client.{CollectionCodecRef, MongoConnection}
import com.mongodb.{MongoClientSettings, ServerAddress}
import org.mongodb.scala.bson.codecs.Macros.createCodecProvider

import scala.jdk.CollectionConverters._

val clientSettings =
  MongoClientSettings.builder()
    .applyToClusterSettings(builder =>
      builder.hosts(
        List(
          new ServerAddress("host1", 27017),
          new ServerAddress("host2", 27017)
        ).asJava
      )
    ).build()

val employeesCol = CollectionCodecRef("myDb", "employees", classOf[Employee], createCodecProvider[Employee]())
val connection = MongoConnection.create1(clientSettings, employeesCol)
```

## CollectionOperator

Notice that the creation of the connection returns `cats.effect.Resource`, which abstracts the acquisition and release
of the resources consumed by creating the `MongoClient`. The usage of such resources provides a `CollectionOperator`,
which it is composed by four main components: `Database`, `Source`, `Single` and `Sink`, and together, they bring all
the necessary methods to operate with the specified collections.

### Multiple Collections

The api is designed to allow using multiple collections (up to 8) in a single connection. This is exposed in the
signatures `create1`, `create2`, `create3`, ..., `create8`, and they a `TupleN` respectively to the number of
collections, see below an example:

```scala
import monix.connect.mongodb.client.{CollectionCodecRef, CollectionDocumentRef, MongoConnection}
import com.mongodb.client.model.Filters
import org.mongodb.scala.bson.codecs.Macros.createCodecProvider
import org.bson.Document
import monix.execution.Scheduler.Implicits.global

import scala.jdk.CollectionConverters._

val business: Document = Document.parse(s"""{"business_name":"MyBusiness", "year": 1995}""")
val employee1 = Employee("Employee1", 21, "Paris")
val employee2 = Employee("Employee2", 29, "Amsterdam")
val businessCol = CollectionDocumentRef(database = "myDb", collection = "business_collection")
val employeesCol =
  CollectionCodecRef("myDb", "employees_collection", classOf[Employee], createCodecProvider[Employee]()) 

// calling `create2` because we want operate with two mongodb collections.
MongoConnection.create2("mongodb://host:27017", (employeesCol, businessCol))
  .use { case (employeesOperator, businessOperator) =>
  for {
    //business logic here
    _ <- employeesOperator.single.insertMany(List(employee1, employee2)) >>
      businessOperator.single.insertOne(business)
    parisEmployeesCount <- employeesOperator.source.count(Filters.eq("city", "Paris"))
    myOnlyBusiness <- businessOperator.source.find(Filters.eq("film_name", "MyBusiness")).headL
  } yield (parisEmployeesCount, myOnlyBusiness)
}.runToFuture
```

## Database

Dealing with _database_ operations is a common use case in any driver. Nevertheless, the `CollectionOperator` comes with
an instance of `MongoDatabase`, allowing to __create__, __drop__, __list__ and __check existence__ of both _databases_
and _collections_,

### Exists

The `MongoDatabase` is just a reference to the target _database_, which may or may not already exist. In case you want
to check that before trying to read or insert any data:

```scala
import monix.eval.Task
import monix.connect.mongodb.MongoDb
import monix.connect.mongodb.client.MongoConnection
import monix.connect.mongodb.client.CollectionCodecRef
import org.mongodb.scala.bson.codecs.Macros.createCodecProvider
import monix.execution.Scheduler.Implicits.global

val employeesCol = CollectionCodecRef("myDb", "employees", classOf[Employee], createCodecProvider[Employee]())
val connection = MongoConnection.create1("mongodb://host:27017", employeesCol)
val isDbPresent = connection.use { colOperator =>
  for {
    isDbPresent <- colOperator.db.existsDatabase("myDb")
    isCollectionPresent <- colOperator.db.existsCollection("my_collection")
  } yield (isDbPresent, isCollectionPresent)
}
// if the db existed you could also check the existence of a particular collection
val isCollectionPresent: Task[Boolean] = MongoDb.existsCollection("myDb", "my_collection")
```

### Create

Create a new collection within a given _database_.

```scala
import org.mongodb.scala.bson.codecs.Macros.createCodecProvider
import monix.connect.mongodb.client.{MongoConnection, CollectionCodecRef}

val employeesCol = CollectionCodecRef("myDb", "employeesCol", classOf[Employee], createCodecProvider[Employee]())
val connection = MongoConnection.create1("mongodb://host:27017", employeesCol).use(_.db.createCollection("db123"))
```

_Notice_ that the collection we are explicitly creating is different to the one we passed to `CollectionRef`, the reason
is because `myDb` is automatically created with the connection, so we don't need to explicitly create it.

### List

List _collection_ and _database_ names.

```scala
import monix.connect.mongodb.client.{CollectionCodecRef, CollectionRef, MongoConnection}
import org.mongodb.scala.bson.codecs.Macros.createCodecProvider
import monix.reactive.Observable

val employeesCol = CollectionCodecRef("myDb", "employeesCol", classOf[Employee], createCodecProvider[Employee]())
val connection = MongoConnection.create1("mongodb://host:27017", employeesCol).use { collOperator =>
  //collection names from all databases
  val collections: Observable[String] = for {
    databaseName <- collOperator.db.listDatabases
    collectionName <- collOperator.db.listCollections(databaseName)
    // calling `listCollections` without arguments, would only
    // return the collections names from the current database 
  } yield collectionName
  collections.count
}

```

### Rename

Rename _collection's_ name on convenience.

```scala
import cats.effect.Resource
import monix.eval.Task
import monix.connect.mongodb.client.{CollectionOperator, MongoConnection}

val connection: Resource[Task, CollectionOperator[Employee]]
val t = connection.use(_.db.renameCollection("oldCollectionName", "newCollectionName"))
```

### Drop

And finally, _drop_ either the entire _database_ or _collection_.

```scala
import cats.effect.Resource
import monix.eval.Task
import monix.connect.mongodb.client.{CollectionOperator, MongoConnection}

val connection: Resource[Task, CollectionOperator[Document]]
connection.use { operator =>
  for {
    _ <- operator.db.dropCollection("db123", "coll123")
    _ <- operator.db.dropDatabase("db123")
  } yield ()
}.runToFuture
```

## Source

The `MongoSource` class abstracts the _read-like_ operations _aggregate_, _count_, _distinct_ and _find_ and others, see
examples for these in next subsections:

### Aggregate

This source _aggregates_ documents according to the specified _aggregation pipeline_. You can find below examples for
performing _group by_ and _unwind_ aggregations.

__Group By__

 ```scala
import org.bson.Document
import com.mongodb.client.model.{Accumulators, Aggregates, Filters}
import cats.effect.Resource
import monix.eval.Task
import monix.connect.mongodb.client.CollectionOperator

val connection: Resource[Task, CollectionOperator[Employee]]
val aggregated: Task[Option[Document]] = connection.use {
  operator =>
    val filter = Aggregates.`match`(Filters.eq("city", "Caracas"))
    val group = Aggregates.group("group", Accumulators.avg("average", "$age"))
    // eventually returns a [[Document]] with the field `average` 
    // that contains the age average of Venezuelan employees.
    operator.source.aggregate(Seq(filter, group)).headOptionL
}
```

__Unwind__

Deconstructs an array field from the input documents to output a document for each element. So each output document is a
copy of the the main document with the value of the array field replaced by the element.

In the below example the _hobbies_ of the specified employee will be deconstructed:

```scala
import com.mongodb.client.model.{Aggregates, Filters}
import monix.connect.mongodb.client.{CollectionCodecRef, MongoConnection}
import monix.execution.Scheduler.Implicits.global
import org.mongodb.scala.bson.codecs.Macros._
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.configuration.CodecRegistry

case class Person(name: String, age: Int, hobbies: Seq[String])

case class UnwoundPerson(name: String, age: Int, hobbies: String)

val codecRegistry: CodecRegistry = fromRegistries(fromProviders(classOf[Person], classOf[UnwoundPerson]))
val col = CollectionCodecRef("myDb", "persons", classOf[Person], codecRegistry)
MongoConnection.create1("mongodb://host:27017", col).use { operator =>
  for {
    _ <- operator.single.insertOne(Person("Mario", 32, List("reading", "running", "programming")))
    unwound <- {
      val filter = Aggregates.`match`(Filters.gte("age", 32))
      val unwind = Aggregates.unwind("$hobbies")
      operator.source.aggregate(Seq(filter, unwind), classOf[UnwoundPerson]).toListL
      /** Returns ->
        * List(
        * UnwoundPerson("Mario", 32, "reading"), 
        * UnwoundPerson("Mario", 32, "running"),
        * UnwoundPerson("Mario", 32, "programming")
        * )
        */
    }
    //
  } yield unwound
}.runToFuture
```

### Count

#### countAll

Counts all the documents, without filtering.

```scala
import cats.effect.Resource
import monix.connect.mongodb.client.{CollectionOperator, MongoConnection}
import monix.eval.Task

val connection: Resource[Task, CollectionOperator[Document]]
val count = connection.use(_.source.countAll)
```

#### count

Counts the number of elements that matched with the given filter.

```scala
import cats.effect.Resource
import com.mongodb.client.model.Filters
import monix.connect.mongodb.client.CollectionOperator
import monix.eval.Task

val connection: Resource[Task, CollectionOperator[Employee]]
// counts the number of employees based in Sydney
val count = connection.use(_.source.count(Filters.eq("city", "Sydney")))
```

### Distinct

Gets the distinct values of the specified field name.

```scala
import cats.effect.Resource
import com.mongodb.client.model.Filters
import monix.connect.mongodb.client.CollectionOperator
import monix.eval.Task

val connection: Resource[Task, CollectionOperator[Employee]]
// count of the distinct cities
val distinctCount = connection.use(_.source.distinct("city", classOf[String]).countL)
```

### Find

#### findAll

Finds all the documents.

```scala
import cats.effect.Resource
import monix.connect.mongodb.client.CollectionOperator
import monix.eval.Task
import monix.reactive.Observable

val connection: Resource[Task, CollectionOperator[Employee]]
val t = connection.use { operator =>
  val allEmployees: Observable[Employee] = operator.source.findAll
  //business logic here
  allEmployees.completedL
}
```

#### find

Finds the documents in the collection that matched the query filter.

```scala
import cats.effect.Resource
import com.mongodb.client.model.Filters
import monix.connect.mongodb.client.CollectionOperator
import monix.eval.Task
import monix.reactive.Observable

val connection: Resource[Task, CollectionOperator[Employee]]
connection.use { operator =>
  val rioEmployees: Observable[Employee] = operator.source.find(Filters.eq("city", "Rio"))
  //business logic here
  rioEmployees.completedL
}.runToFuture
```

#### findOneAndDelete

Atomically _find_ a document and _remove_ it.

```scala
import cats.effect.Resource
import com.mongodb.client.model.Filters
import monix.connect.mongodb.client.{CollectionOperator, MongoConnection}
import monix.eval.Task
import monix.reactive.Observable
import org.bson.conversions.Bson

// finds and deletes one Cracow employee older than 66 and
val filter = Filters.and(Filters.eq("city", "Cracow"), Filters.gt("age", 66))
val connection: Resource[Task, CollectionOperator[Employee]]
connection.use { operator =>
  val retiredEmployee: Task[Option[Employee]] = operator.source.findOneAndDelete(filter)
  //business logic here
  retiredEmployee
}.runToFuture
 ```

#### findOneAndReplace

Atomically _find_ a document and _replace_ it.

```scala
import cats.effect.Resource
import com.mongodb.client.model.Filters
import monix.connect.mongodb.client.CollectionOperator
import monix.eval.Task

val filter = Filters.and(Filters.eq("city", "Cadiz"), Filters.eq("name", "Gustavo"))
val replacement = Employee(name = "Alberto", city = "Cadiz", age = 33)
val connection: Resource[Task, CollectionOperator[Employee]]
connection.use { operator =>
  val replacedEmployee: Task[Option[Employee]] = operator.source.findOneAndReplace(filter, replacement)
  //business logic here
  replacedEmployee
}.runToFuture
```

#### findOneAndUpdate

Atomically _find_ a document and _update_ it.

```scala
import cats.effect.Resource
import com.mongodb.client.model.{Filters, Updates}
import monix.connect.mongodb.client.CollectionOperator
import monix.eval.Task

val filter = Filters.and(Filters.eq("city", "Moscou"), Filters.eq("name", "Vladimir"))
val update = Updates.inc("age", 1)
val connection: Resource[Task, CollectionOperator[Employee]]
connection.use(_.source.findOneAndUpdate(filter, update)).runToFuture
```

## Single & Sinks

The `Single` and `Sink` components are documented together since they both implement exactly the same set of
operations _insert_, _delete_, _replace_ and _update_, although with the difference that the first executes the
operation in single task and the latter pipes the elements of the stream to a _Monix_ `Consumer` to execute them.

The following sub-sections represent the list of different _operations_ and _sinks_ available to use, with a small
example on each one.

### Insert

#### insertOne

Inserts the provided _document_ to the specified collection.

_Single_:

```scala
import cats.effect.Resource
import monix.connect.mongodb.domain.InsertOneResult
import monix.connect.mongodb.client.CollectionOperator
import monix.eval.Task

val employee: Employee = Employee("Luke", "London", 41)
val connection: Resource[Task, CollectionOperator[Employee]]
val t: Task[InsertOneResult] = connection.use(_.single.insertOne(employee))
```

_Sink_:

```scala
import cats.effect.Resource
import monix.connect.mongodb.client.CollectionOperator
import monix.eval.Task
import monix.reactive.Observable

val employees = List(Employee("Luke", "London", 41))
val connection: Resource[Task, CollectionOperator[Employee]]
connection.use { operator =>
  Observable.from(employees) // Observable[Employee]
    .consumeWith(operator.sink.insertOne())
}.runToFuture
```

#### insertMany

Inserts the provided list of documents to the specified collection.

_Single_:

```scala
import cats.effect.Resource
import monix.connect.mongodb.domain.InsertManyResult
import monix.connect.mongodb.client.CollectionOperator
import monix.eval.Task

val employees: List[Employee] = List(Employee("Jim", "Vienna", 33), Employee("Albert", "Vienna", 45))

val connection: Resource[Task, CollectionOperator[Employee]]
val t: Task[InsertManyResult] = connection.use(_.single.insertMany(employees))
```

_Sink_:

```scala
import cats.effect.Resource
import monix.connect.mongodb.client.CollectionOperator
import monix.eval.Task
import monix.reactive.Observable

val employees: List[Employee] = List(
  Employee("Salomon", "Berlin", 23),
  Employee("Victor", "Berlin", 54),
  Employee("Gabriel", "Paris", 35)
)
val connection: Resource[Task, CollectionOperator[Employee]]
connection.use { operator =>
  Observable.from(employees) // Observable[Employee]
    .bufferTumbling(3) // Observable[Seq[Employee]]
    .consumeWith(operator.sink.insertMany())
}.runToFuture
```

### Delete

#### deleteOne

Removes at most one _document_ from the _collection_ that matches the given _filter_. If no documents match, the _
collection_ is not modified.

_Single_:

```scala
import cats.effect.Resource
import com.mongodb.client.model.Filters
import monix.connect.mongodb.domain.DeleteResult
import monix.connect.mongodb.client.CollectionOperator
import monix.eval.Task

val filter = Filters.eq("name", "Bob")
val connection: Resource[Task, CollectionOperator[Employee]]
val t: Task[DeleteResult] = connection.use(_.single.deleteOne(filter))
```

_Sink_:

```scala
import com.mongodb.client.model.Filters
import cats.effect.Resource
import monix.connect.mongodb.client.CollectionOperator
import monix.eval.Task
import monix.reactive.Observable

val connection: Resource[Task, CollectionOperator[Employee]]
connection.use { operator =>
  // employees to be deleted from the collection
  Observable.from(List("Lauren", "Marie", "Simon")) // Observable[String] 
    .map(name => Filters.eq("name", name)) // Observable[Bson]
    .consumeWith(operator.sink.deleteOne())
}.runToFuture
```

#### deleteMany

Removes all documents from the _collection_ that match the given query filter. Again if no _documents_ match, the _
collection_ is not modified.

_Single_:

```scala
import cats.effect.Resource
import com.mongodb.client.model.Filters
import monix.connect.mongodb.domain.DeleteResult
import monix.connect.mongodb.client.CollectionOperator
import monix.eval.Task

val connection: Resource[Task, CollectionOperator[Employee]]
// delete all employees older than 65 y.o
val filter = Filters.gt("age", 65)
val t: Task[DeleteResult] = connection.use(_.single.deleteMany(filter))
```

_Sink_:

```scala
import cats.effect.Resource
import com.mongodb.client.model.Filters
import monix.connect.mongodb.client.CollectionOperator
import monix.eval.Task
import monix.reactive.Observable

val connection: Resource[Task, CollectionOperator[Employee]]
connection.use { operator =>
  // all employees from `Germany`, `Italy` or `Turkey` were fired (deleted)
  Observable.from(List("Germany", "Italy", "Turkey")) // Observable[String] 
    .map(city => Filters.eq("city", city)) // Observable[Bson]
    .consumeWith(operator.sink.deleteMany())
}.runToFuture
```

### Replace

#### replaceOne

Replaces a _document_ in the _collection_ according to the specified arguments.

_Single_:

```scala
import com.mongodb.client.model.Filters
import monix.connect.mongodb.domain.DeleteResult
import cats.effect.Resource
import monix.connect.mongodb.client.CollectionOperator
import monix.eval.Task

val connection: Resource[Task, CollectionOperator[Employee]]
// assuming that `Bob` exists in the collection it gets 
// retired and replaced by `Alice`
val filter = Filters.eq("name", "Bob")
val replacement = Employee("Alice", "France", 43)
val t: Task[DeleteResult] = connection.use(_.single.replaceOne(filter, replacement))
```

_Sink_:

```scala
import cats.effect.Resource
import com.mongodb.client.model.Filters
import monix.connect.mongodb.client.CollectionOperator
import monix.eval.Task
import monix.reactive.Observable
import org.bson.conversions.Bson

val connection: Resource[Task, CollectionOperator[Employee]]
// Presumably the collection have employees 1 and 2, then they are replaced by 3 and 4
val replacements: Seq[(Bson, Employee)] =
  List(
    (Filters.eq("name", "Employee1"), Employee("Employee3", 43, "Rio")),
    (Filters.eq("name", "Employee2"), Employee("Employee4", 37, "Rio"))
  )
connection.use { operator =>
  Observable.from(replacements) // Observable[(Bson, Employee)]
    .consumeWith(operator.sink.replaceOne())
}.runToFuture
```

### Update

#### updateOne

Update a single document in the collection according to the specified arguments.

_Single_:

```scala
import cats.effect.Resource
import com.mongodb.client.model.{Filters, Updates}
import monix.connect.mongodb.domain.UpdateResult
import monix.connect.mongodb.client.CollectionOperator
import monix.eval.Task

val connection: Resource[Task, CollectionOperator[Employee]]
/** Updates Bob's employee record for its anniversary, an empty option is returned 
  * when there was no record that matched with the given filter. */
val filter = Filters.eq("name", "Bob")
val anniversaryUpdate = Updates.inc("age", 1)
val t: Task[UpdateResult] = connection.use(_.single.updateOne(filter, anniversaryUpdate))
```

_Sink_:

```scala
import cats.effect.Resource
import com.mongodb.client.model.{Filters, Updates}
import monix.connect.mongodb.client.CollectionOperator
import monix.eval.Task
import monix.reactive.Observable
import org.bson.conversions.Bson

val connection: Resource[Task, CollectionOperator[Employee]]
connection.use { operator =>
  val filter = Filters.eq("city", "Porto")
  val update = Updates.set("city", "Lisbon")
  val updateElements: Seq[(Bson, Bson)] = List.fill(4)((filter, update))
  // imagine that a company wants to send four of its employees from Porto to Lisbon
  Observable.from(updateElements) // Observable[(Bson, Bson)]
    .consumeWith(operator.sink.updateOne())
}.runToFuture
```

#### updateMany

Update all documents in the collection according to the specified arguments.

_Single_:

```scala
import cats.effect.Resource
import com.mongodb.client.model.{Filters, Updates}
import monix.connect.mongodb.domain.UpdateResult
import monix.connect.mongodb.client.CollectionOperator
import monix.eval.Task

val connection: Resource[Task, CollectionOperator[Employee]]
val filter = Filters.eq("city", "Spain")
val update = Updates.push("hobbies", "Football")
val t: Task[UpdateResult] = connection.use(_.single.updateMany(filter, update))
```

_Sink_:

```scala
import cats.effect.Resource
import com.mongodb.client.model.{Filters, Updates}
import monix.connect.mongodb.client.CollectionOperator
import monix.eval.Task
import monix.reactive.Observable
import org.bson.conversions.Bson

val connection: Resource[Task, CollectionOperator[Employee]]
connection.use { operator =>
  val cities: Set[String] = Set("Seattle", "Nairobi", "Dakar")
  Observable.from(cities) // Observable[String]
    .map(city => (Filters.eq("city", city), Updates.pull("hobbies", "Table Tennis"))) // Observable[(Bson, Bson)]
    .consumeWith(operator.sink.updateMany())
}.runToFuture
```



 
