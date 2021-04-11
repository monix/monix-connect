---
id: mongodb
title: MongoDB
---

## Introduction
  
_MongoDB_ is a _document database_ which means that stores data in _JSON-like documents_ and is much more expressive and powerful than the traditional _row/column_ model.
 It has a rich and expressive query language that allows you to filter and sort by any field with 
 support for aggregations and other modern use-cases.
 The _Monix MongoDB_ connector offers a _non blocking_ and _side-effectful_ api that relies on the the underlying the [MongoDB Java Reactive Streams](https://docs.mongodb.com/drivers/reactive-streams) driver. 
 The library is designed in four different parts:
  - __Database:__ Used to manage and dealing with _mongo databases_ and _collections_.
  - __Operation:__ It exposes single operations to delete, insert, replace and update _collections_.
  - __Sink:__ Implements the same operations as the _Operation_ api, but it in a streaming fashion.
  - __Source:__ Used to fetch data from the collections with _aggregate_, _count_, _distinct_ and _find_.

 Each of these components is explained in detail on the following sections, being _Operation_ and _Sink_ joined in the same section since they share 
 
## Dependency

Add the following dependency to get started:
```scala
libraryDependencies += "io.monix" %% "monix-mongodb" % "0.6.0"
```

## Database

Dealing with _database_ management is a quite common use case in any driver. Nevertheless, this connector provides 
a set of methods to interact with _MongoDB_ domain, exposed in the _object_ `monix.connect.mongodb.MongoDb` with the methods to __create__, __drop__, __list__ and __check existence__ of both _databases_ and _collections_.                                                                        

Let's then see some examples on how to use the mentioned utilities.

Firstly, it is needed to create a connection to the _database_, which in this case we're using a local _standalone MongoDB instance_, but if you need to 
 connect to a _Replica Set_, a _Sharded Cluster_ or use some options such _TLS/SSL_, you would better refer to the [MongoDB documentation](https://mongodb.github.io/mongo-java-driver/4.1/driver-reactive/tutorials/connect-to-mongodb/).

```scala
import com.mongodb.reactivestreams.client.{MongoClients, MongoDatabase}

val client: MongoClient = MongoClients.create(s"mongodb://localhost:27017")

// now you can request a database instance by just typing its name
val db: MongoDatabase = client.getDatabase("mydb") 
```

### Exists

__Important__ to incise that the `MongoDatabase` is just a reference to the target _database, 
which may or may not already exist. In case you want to check that before trying to read or insert any data:

```scala
import monix.connect.mongodb.MongoDb

val isDbPresent: Task[Boolean] = MongoDb.existsDatabase(db)

// if the db existed you could also check the existence of a particular collection
val isCollectionPresent: Task[Boolean] = MongoDb.existsCollection(db, "myCollection")
```

### Create 

Create a new collection within a given _database_.

```scala
val created: Task[Boolean] = MongoDb.createCollection(db, "myCollection")
```

_Notice_ that creating a collection within a _database_ forces the second one to be created as well (in case it did not exist before).

### List

List _collection_ and _database_ names.

```scala
val databases: Observable[String] = MongoDb.listDatabases(client)
val collections: Observable[String] = MongoDb.listCollections(db)
```

### Rename

Rename _collection's_ name on convenience.

```scala
val isRenamed: Task[Boolean] = MongoDb.renameCollection(db, "oldCollection", "newCollection")
```

### Drop

And finally _drop_ either the whole _database_ or a single _collection_.

```scala
val databases: Task[Boolean] = MongoDb.dropDatabase(db)
val collections: Task[Boolean] = MongoDb.dropCollection(db, "newCollection")
```

## Collections 

At the time you are directly interacting with the collections either for reading or writing,
 you would always need an instance of a `MongoCollection` which represents the target collection.
 
Such object is typed by default with `org.bson.Document` but it also supports automatic _codec derivation_ which allows to specify a class document to that collection.

Both have some pros and cons, the first one provides more flexibility to working with any type of document that the collection may contain and also to insert following different formats. 

Creating an instance of a _collection_ without _codec derivation_ would be just like below code:

 ```scala
import com.mongodb.reactivestreams.client.MongoCollection
import org.bson.Document

val col: MongoCollection[Document] = db.getCollection("myCollection")
```

On the other hand if you use _codec derivation_, your generated collection will stick to that codec either for _reading_ or _writing_.
To do so, it is necessary first to define the class that you'll later supply to the `codecRegistry`, as an example we are going to use the following one:
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

Now that we have the instance of `MongoCollection` we're ready to start operating with such _collection_. 
Notice that in the below examples we are going to use the _employees collection_ that just was created,
 meaning that our input and output documents will be of type `Employee`.

## Operations & Sinks
As it was mentioned at the beginning, the documentation of _Operation_ (`MongoOp`) and _Sink_ (`MongoSink`) is explained in the same section
since these expose exactly the same set of operations _insert_, _delete_, _replace_ and _update_, although they approach differs on single (`Task`) vs multiple elements (`Observable`).

The following sub-sections represent the list of different _operations_ and _sinks_ available to use, with a small example for each one.
Notice that the objects `MongoOp` and `MongoSink` are not imported in each of the examples since its assumed that these objects are already in the scope, 
if you want to import them they are located in the package: `monix.connect.mongodb`.
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
Removes at most one _document_ from the _collection_ that matches the given _filter_. If no documents match, the _collection_ is not modified.

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
Removes all documents from the _collection_ that match the given query filter. Again if no _documents_ match, the _collection_ is not modified.

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
with some exceptions that also will modify the collection (_findOneAnd* Delete_, _Replace_ or _Update_),
 these three exceptions were put to _MongoSource_ rather than to _MongoOp_ 
 since its main purpose is to _fetch_ a _document_ but then they also alter it from the _collection_. 
 
 ### Aggregate
     
 This source _aggregates_ documents according to the specified _aggregation pipeline_. 
 You can find below examples for performing _group by_ and a _unwind_ aggregations.

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

Deconstructs an array field from the input documents to output a document for each element. 
So each output document is a copy of the the main document with the value of the array field replaced by the element.

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
val t: Task[Employee]  = MongoSource.findOneAndReplace(col, filter, replacement)
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


 
