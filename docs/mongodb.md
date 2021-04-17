---
id: mongodb title: MongoDB
---

## Introduction

_MongoDB_ is a _document database_, in which the data is stored in _JSON-like documents_ differing to the traditional _
row/column_ model. It has a rich and expressive query language that allows you to filter and sort by any field with
support for aggregations and other modern use-cases. The _Monix MongoDB_ connector offers a _reactive_, _non blocking_
and _resource safe_ api, which relies in the underlying
the [MongoDB Java Reactive Streams](https://docs.mongodb.com/drivers/reactive-streams) driver. The library is designed
in four different parts:

- __Database:__ Used to manage and dealing with _mongo databases_ and _collections_.
- __Operation:__ It exposes single operations to delete, insert, replace and update _collections_.
- __Sink:__ Implements the same operations as the _Operation_ api, but it in a streaming fashion.
- __Source:__ Used to fetch data from the collections with _aggregate_, _count_, _distinct_ and _find_.

Each of these components is explained in detail in the following sections:

## Dependency

Add the following dependency to get started:

```scala
libraryDependencies += "io.monix" %% "monix-mongodb" % "0.6.0"
```

## Collection Reference

Before creating the connection, we would need to have a reference to the `Collection` that we want to reach and to use
for storing and reading documents.

Such collection reference is identified by the _database_ and _collection_ names, and represented in code by
the `CollectionRef` _sealed trait_, which is inherited by `CollectionCodecRef` and `CollectionDocRef`.

### CollectionCodecRef

The most practical way of representing documents in _Scala_ code is by defining them as _case class_, in which then they
would need to be derived as `CodecProvider`. To do so, we could import implicit codec conversions
from `import org.mongodb.scala.bson.codecs.Macros._`.

For didactic purposes we have defined an `Employee` with a hypothetical `employees` collection that will be used to show
how to create a collection reference with our custom _Codec_ that will later be used to create the `MongoConnection`.

```scala
import org.mongodb.scala.bson.codecs.Macros.createCodecProvider
import monix.connect.mongodb.client.CollectionCodecRef
import monix.connect.mongodb.client.CollectionRef

case class Employee(name: String, age: Int, city: String)

val employee = Employee("Bob", 29, "Barcelona")
val employeesCol: CollectionRef[Employee] =
  CollectionCodecRef("myDb", "employees", classOf[Employee], createCodecProvider[Employee]())
```

### CollectionDocumentRef

On the other hand, one could also use `org.bson.Document` as a generic type for representing a _Document_. In that
case `CollectionDocumentRef` would be used, which only require the _db_ and _collection_ name to be created:

```scala
import org.mongodb.scala.bson.codecs.Macros.createCodecProvider
import monix.connect.mongodb.client.CollectionDocumentRef
import monix.connect.mongodb.client.CollectionRef
import org.bson.Document

val sampleDocument = Document.parse("""{"film_name":"Jumanji", "year": 1995 }""")
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
import monix.eval.Task

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
import com.mongodb.{MongoClientSettings, ServerAddress}
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
val connection = MongoConnection.create2("mongodb://host:27017", (employeesCol, businessCol))

connection.use { case (employeesOperator, businessOperator) =>
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
import monix.eval.Task
import monix.connect.mongodb.MongoDb
import monix.connect.mongodb.client.MongoConnection
import monix.connect.mongodb.client.CollectionRef

val employeesCol = CollectionCodec("myDb", "employeesCol", classOf[Employee], createCodecProvider[Employee]())
val connection = MongoConnection.create1("mongodb://host:27017", employeesCol).use(_.db.createCollection("db123"))
```

_Notice_ that the collection we are explicitly creating is different to the one we passed to `CollectionRef`, the reason
is because `myDb` is automatically created with the connection, so we don't need to explicitly create it.

### List

List _collection_ and _database_ names.

```scala
import monix.connect.mongodb.client.MongoConnection
import monix.connect.mongodb.client.CollectionRef
import monix.reactive.Observable

val employeesCol = CollectionCodec("myDb", "employeesCol", classOf[Employee], createCodecProvider[Employee]())
val connection = MongoConnection.create1("mongodb://host:27017", employeesCol).use { collOperator =>
  //collection names from all databases
  val collections: Observable[String] = for {
    databaseName <- collOperator.db.listAllDatabases
    collectionName <- collOperator.db.listCollections(databaseName)
    // calling `listCollections` without arguments, would only
    // return the collections names from the current list 
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

val connection: Resource[Task, CollectionOperator[Employee]]
val t = connection.use { operator =>
  for {
    _ <- operator.db.dropCollection("db123", "coll123")
    _ <- operator.db.dropDatabase("db123")
  } yield ()
}

```

## Collections

At the time you are directly interacting with the collections either for reading or writing, you would always need an
instance of a `MongoCollection` which represents the target collection.

Such object is typed by default with `org.bson.Document` but it also supports automatic _codec derivation_ which allows
to specify a class document to that collection.

Both have some pros and cons, the first one provides more flexibility to working with any type of document that the
collection may contain and also to insert following different formats.

Creating an instance of a _collection_ without _codec derivation_ would be just like below code:

 ```scala
import com.mongodb.reactivestreams.client.MongoCollection
import org.bson.Document

val col: MongoCollection[Document] = db.getCollection("myCollection")
```

On the other hand if you use _codec derivation_, your generated collection will stick to that codec either for _reading_
or _writing_. To do so, it is necessary first to define the class that you'll later supply to the `codecRegistry`, as an
example we are going to use the following one:

```scala
case class Employee(name: String, age: Int, nationality: String, activities: List[String] = List.empty)
```

Then create a `_codecRegistry_ with the _case class_ we just defined and supply it to the _collection_.

 ```scala
import com.mongodb.reactivestreams.client.{MongoCollection, MongoClient, MongoClients, MongoDatabase}
import org.bson.Document
import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY

import org.mongodb.scala.bson.codecs.Macros._
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}

val client: MongoClient = MongoClients.create(s"mongodb://localhost:27017")
val db: MongoDatabase = client.getDatabase("mydb")

val codecRegistry = fromRegistries(fromProviders(classOf[Employee]), DEFAULT_CODEC_REGISTRY)

val col: MongoCollection[Employee] = db.getCollection("myCollection", classOf[Employee])
  .withCodecRegistry(codecRegistry)
```

Now that we have the instance of `MongoCollection` we're ready to start operating with such _collection_. Notice that in
the below examples we are going to use the _employees collection_ that just was created, meaning that our input and
output documents will be of type `Employee`.

## Operations & Sinks

As it was mentioned at the beginning, the documentation of _Operation_ (`MongoOp`) and _Sink_ (`MongoSink`) is explained
in the same section since these expose exactly the same set of operations _insert_, _delete_, _replace_ and _update_,
although they approach differs on single execution (`Task`) vs multiple (`Observable`).

The following sub-sections represent the list of different _operations_ and _sinks_ available to use, with a small
example for each one. Notice that the objects `MongoOp` and `MongoSink` are not imported in each of the examples since
its assumed that these objects are already in the scope, if you want to import them they are located in the
package: `monix.connect.mongodb`.

### Insert

#### insertOne

Inserts the provided document to the specified collection.

_Operation_:

```scala
import monix.connect.mongodb.domain.InsertOneResult
import monix.connect.mongodb.MongoOp

val employee: Employee = Employee("Luke", "London", 29)

val t: Task[InsertOneResult] = MongoOp.insertOne(col, employee)
```

_Sink_:

```scala
import monix.connect.mongodb.MongoSink

val employees: List[Employee] = List(Employee("Ryan", "Singapore", 25), Employee("Nil", "Singapore", 55))
Observable.from(employees) // Observable[Employee]
  .consumeWith(MongoSink.insertOne(col))
```

#### insertMany

Inserts the provided list of documents to the specified collection.

_Operation_:

```scala
import monix.connect.mongodb.domain.InsertManyResult
import monix.connect.mongodb.MongoOp

val employees: List[Employee] = List(Employee("John", "Vienna", 33), Employee("Albert", "Vienna", 45))

val t: Task[InsertManyResult] = MongoOp.insertMany(col, employees)
```

_Sink_:

```scala
import monix.connect.mongodb.MongoSink

val l1: List[Employee] = List(Employee("Gerard", "Paris", 36), Employee("Gabriel", "Paris", 35))
val l2: List[Employee] = List(Employee("Salomon", "Berlin", 23), Employee("Victor", "Berlin", 54))

Observable.from(List(l1, l2)) // Observable[List[Employee]]
  .consumeWith(MongoSink.insertMany(col))
```

### Delete

#### deleteOne

Removes at most one _document_ from the _collection_ that matches the given _filter_. If no documents match, the _
collection_ is not modified.

_Operation_:

```scala
import com.mongodb.client.model.Filters
import monix.connect.mongodb.domain.DeleteResult
import monix.connect.mongodb.MongoOp

val filter = Filters.eq("name", "Bob")
// returns an empty option when for example the filter did not find any match 
val t: Task[DeleteResult] = MongoOp.deleteOne(col, filter)
```

_Sink_:

```scala
import com.mongodb.client.model.Filters
import monix.connect.mongodb.MongoSink

val employeeNames: List[String]

// per each element, one employee will be deleted
Observable.from(employees) // Observable[String] 
  .map(name => Filters.eq("name", name)) // Observable[Bson]
  .consumeWith(MongoSink.deleteOne(col))
```

#### deleteMany

Removes all documents from the _collection_ that match the given query filter. Again if no _documents_ match, the _
collection_ is not modified.

_Operation_:

```scala
import com.mongodb.client.model.Filters
import monix.connect.mongodb.domain.DeleteResult
import monix.connect.mongodb.MongoOp

// delete all employees older than 65 y.o
val t: Task[DeleteResult] = MongoOp.deleteMany(col, Filters.gt("age", 65))
```

_Sink_:

```scala
import com.mongodb.client.model.Filters
import monix.connect.mongodb.MongoSink

// all employees from `Germany`, `Italy` or `Turkey` would be deleted (fired)  
Observable.from(List("Germany", "Italy", "Turkey")) // Observable[List[String]]
  .map(location => Filters.eq("location", location)) // Observable[List[Bson]]
  .consumeWith(MongoSink.deleteMany(col))
```

### Replace

#### replaceOne

Replace a _document_ in the _collection_ according to the specified arguments.

_Operation_:

```scala
import com.mongodb.client.model.Filters
import monix.connect.mongodb.domain.UpdateResult
import monix.connect.mongodb.MongoOp

// assuming that `Bob` exists in the collection
val bob = Employee(name = "Bob", location = "France", age = 65)

val alice = Employee(name = "Alice", location = "France", age = 43)
val filter: Bson = Filters.eq("name", "Bob")

// Bob retires and is replaced by Alice
val t: Task[UpdateResult] = MongoOp.replaceOne(col, filter, alice)
```

_Sink_:

```scala
import com.mongodb.client.model.Filters
import monix.connect.mongodb.MongoSink

// presumably the collection have employees 1 and 2, then they are replaced by 3 and 4
val t1 = (Filters.eq("name", "Employee1"), Employee("Employee3", 43, "Rio"))
val t2 = (Filters.eq("name", "Employee2"), Employee("Employee4", 37, "Rio"))
val replacements: Seq[(Bson, Employee)] = List(t1, t2)

Observable.from(replacements) // Observable[(Bson, Employee)]
  .consumeWith(MongoSink.replaceOne(col)).runSyncUnsafe()
```

### Update

#### updateOne

Update a single document in the collection according to the specified arguments.

_Operation_:

```scala
import com.mongodb.client.model.{Filters, Updates}
import monix.connect.mongodb.domain.UpdateResult
import monix.connect.mongodb.MongoOp

val filter = Filters.eq("name", "Bob")
val anniversaryUpdate = Updates.inc("age", 1)

/** updates Bob's employee record for its anniversary, an empty option is returned 
  * when there was no record that matched with the given filter. */
val t: Task[UpdateResult] = MongoOp.updateOne(col, filter, anniversaryUpdate)
```

_Sink_:

```scala
import com.mongodb.client.model.{Filters, Updates}
import monix.connect.mongodb.MongoSink

val filter = Filters.eq("city", "Porto")
val update = Updates.set("city", "Lisbon")
val l: Seq[(Bson, Bson)] = List.fill(4)((filter, update))

// imagine that a company wants to send four of its employees from Porto to Lisbon
Observable.from(l) // Observable[(Bson, Bson)]
  .consumeWith(MongoSink.updateOne(col))
```

#### updateMany

Update all documents in the collection according to the specified arguments.

_Operation_:

```scala
import com.mongodb.client.model.{Filters, Updates}
import monix.connect.mongodb.domain.UpdateResult
import monix.connect.mongodb.MongoOp

/** imagine that the company in Bombay bought a Table Tennis table, 
  * and we want to add it to list of activities of all the employees of that area */
val filter = Filters.eq("city", "Mumbai")
val update = Updates.push("activities", "Table Tennis")
val t: Task[UpdateResult] = MongoOp.updateMany(col, filter, update)
```

_Sink_:

```scala
import com.mongodb.client.model.{Filters, Updates}
import monix.connect.mongodb.MongoSink

val cities: Set[String] = Set("Seattle", "Nairobi", "Dakar")

Observable.from(cities) // Observable[String]
  .map(city => (Filters.eq("city", city), Updates.pull("activities", "Table Tennis"))) // Observable[(Bson, Bson)]
  .consumeWith(MongoSink.updateMany(col))
```

## Sources

The `MongoSource` object abstracts the mongodb _read-like_ operations (_aggregate_, _count_, _distinct_ and _find_)
with some exceptions that also will modify the collection (_findOneAnd* Delete_, _Replace_ or _Update_), these three
exceptions were put to _MongoSource_ rather than to _MongoOp_
since its main purpose is to _fetch_ a _document_ but then they also alter it from the _collection_.

### Aggregate

This source _aggregates_ documents according to the specified _aggregation pipeline_. You can find below examples for
performing _group by_ and a _unwind_ aggregations.

__Group By__

 ```scala
import org.bson.Document
import monix.connect.mongodb.MongoSource
import com.mongodb.client.model.{Accumulators, Aggregates, Filters}

val filter = Aggregates.`match`(Filters.eq("city", "Caracas"))
val group = Aggregates.group("group", Accumulators.avg("average", "$age"))

// eventually returns a [[Document]] with the field `average` that contains the age average of Venezuelan employees.
val aggregated: Task[Option[Document]] = MongoSource.aggregate[Employee](col, Seq(filter, group)).headOptionL
```

__Unwind__

Deconstructs an array field from the input documents to output a document for each element. So each output document is a
copy of the the main document with the value of the array field replaced by the element.

In the below example the _activities_ of the specified employee will be deconstructed.

```scala
import monix.connect.mongodb.MongoSource
import com.mongodb.client.model.{Aggregates, Filters}
import org.bson.Document
import com.mongodb.reactivestreams.client.MongoCollection
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}

// notice that the activities new deconstructed class is a string type
case class UnwoundEmployee(name: String, age: Int, city: String, activities: String)

// the codec registry will have to include to include the class [[UnwoundEmployee]] to the collection's codec registry
val codecRegistry = fromRegistries(fromProviders(classOf[Employee], classOf[UnwoundEmployee]), DEFAULT_CODEC_REGISTRY)
val col: MongoCollection[Employee] = db.getCollection("myCollection", classOf[Employee]).withCodecRegistry(codecRegistry)


val filter = Aggregates.`match`(Filters.eq("name", "James Bond"))
val unwind = Aggregates.unwind("$activities")

// the number of elements emitted will be equal to the number of activities that employee had set
val unwound1: Observable[UnwoundEmployee] = MongoSource.aggregate[Employee, UnwoundEmployee](col, Seq(filter, unwind), classOf[UnwoundEmployee])

// alternatively we can expect a generic [[Document]] if only the collection's class type is specified, and not the deconstructed class
val unwound2: Observable[Document] = MongoSource.aggregate[Employee](col, Seq(filter, unwind))
```

### Count

#### countAll

Counts all the documents (with no filters)

```scala
import monix.connect.mongodb.MongoSource

val collectionSize: Task[Long] = MongoSource.countAll(col).runSyncUnsafe()
```

#### count

Counts the number of elements that matched with the given filter.

```scala
import com.mongodb.client.model.Filters

// counts the number of employees based in Sydney
val filer: Bson = Filters.eq("city", "Sydney")
val nElements: Task[Long] = MongoSource.count(col, filer)
```

### Distinct

Gets the distinct values of the specified field name.

```scala
import monix.connect.mongodb.MongoSource

// it accepts the field name that wants to be distinct by and its respective class type
val distinct: Observable[String] = MongoSource.distinct(col, "city", classOf[String])
```

### Find

#### findAll

Finds all the documents.

```scala
import monix.connect.mongodb.MongoSource

val ob: Observable[Employee] = MongoSource.findAll[Employee](col)
```

#### find

Finds the documents in the collection that matched the query filter.

```scala
import com.mongodb.client.model.Filters
import monix.connect.mongodb.MongoSource

val filter = Filters.eq("city", "San Francisco")
val ob: Observable[Employee] = MongoSource.find[Employee](col, filter)
```

#### findOneAndDelete

Atomically _find_ a document and _remove_ it.

   ```scala
import com.mongodb.client.model.Filters
import monix.connect.mongodb.MongoSource

// finds and deletes one Cracow employee older than 66 and
val filter = Filters.and(Filters.eq("city", "Cracow"), Filters.gt("age", 66))
val t: Task[Employee] = MongoSource.findOneAndDelete(col, filter)
 ```

#### findOneAndReplace

Atomically _find_ a document and _replace_ it.

```scala
import com.mongodb.client.model.Filters
import monix.connect.mongodb.MongoSource

val filter = Filters.and(Filters.eq("city", "Cadiz"), Filters.eq("name", "Gustavo"))
val replacement = Employee(name = "Alberto", city = "Cadiz", age = 33)
val t: Task[Employee] = MongoSource.findOneAndReplace(col, filter, replacement)
```

#### findOneAndUpdate

Atomically _find_ a document and _update_ it.

```scala
import com.mongodb.client.model.{Filters, Updates}
import monix.connect.mongodb.MongoSource

val filter = Filters.and(Filters.eq("city", "Moscou"), Filters.eq("name", "Vladimir"))
val update = Updates.inc("age", 1)
val t: Task[Employee] = MongoSource.findOneAndUpdate(col, filter, update)
```


 
