---
id: akka
title: Akka Streams
---

## Introduction
  
This module makes interoperability with akka streams easier by simply defining implicit extended classes for reactive stream conversions between akka and monix.

The [reactive streams](https://github.com/reactive-streams/reactive-streams-jvm/blob/v1.0.3/README.md) specification is an standard to follow for those 
JVM libraries that aims to provide asynchronous stream processing with non-blocking back pressure.
As you might probably know, both _Akka Streams_ and _[Monix Reactive](https://monix.io/api/3.2/monix/reactive/index.html)_ follows the same standard, meaning that it is possible to convert between 
their own different stream data types (since both implement the `Publisher` and `Subscriber` contract). 

So this module aims to provide a nice and easy inter-operability between the two mentioned libraries, and to 
achieve that it provides extended conversion methods. 
  These implicit extended methods can be imported from: `monix.connect.akka.stream.Converters._`.
Therefore, under the scope of the import, the signatures `.asObservable` and `.asConsumer` will be available from `Source`, `Flow`, and `Sink` instances, 
whereas `asSource` and `asSink` would be for the monix `Observable` and `Consumer` 

The below table shows that in more detail:  

  | _Akka_ | _Monix_ | _Akka &rarr; Monix_ | _Akka &larr; Monix_ |
  | :---: | :---: | :---: | :---: | 
  | _Source[+In, +Mat]_ | _Observable[+In]_ | `source.asObservable[In]` | `observable.asSource[In]` |
  | _Flow[-In, +Out, +Mat]_ | _Consumer[-In, +Out]_ | `flow.asConsumer[Out]` | - |
  | _Sink[-In, +Out <: Future[Mat]]_ | _Consumer[-In, +Mat]_ | `sink.asConsumer[Mat]` | `consumer.asSink[In]` |

Note that when calling the methods it is not needed to pass the _type parameter_ (as it has been explicitly indicated in the example table), the compiler will infer it for you.

Also, in order to perform these conversion it is required to have an implicit instance of `akka.stream.Materializer` and `monix.execution.Scheduler` in the scope.

## Dependency

Add the following dependency to get started:
```scala 
libraryDependencies += "io.monix" %% "monix-akka" % "0.5.0"
```

## Getting started

The only three things we will need to perform these conversions would be the implicit conversion, and an instance of the monix `Scheduler` and the akka `ActorMaterializer`. 
```scala
import monix.connect.akka.stream.Converters._
import monix.execution.Scheduler.Implicits.global

val actorSystem: ActorSystem = ActorSystem("Akka-Streams-InterOp")  
implicit val materializer = ActorMaterializer() //setting actorSystem as implicit variable might have ben enough
```

### Akka &rarr; Monix

#### asObservable

Let's see how easy can be converting an `Source[+In, +Mat]` to `Observable[+In]`:

```scala
//given
val source: Source[Int] = Source.from(1 until 50)

//when
val ob: Observable[Int] = source.asObservable //`asObservable` converter as extended method of source.

//then
ob.toListL.runSyncUnsafe() should contain theSameElementsAs elements
```

In this case we have not needed to consume the `Observable` since we directly used an operator that collects 
to a list `.toList`, but note that in case you need to use an specific consumer, you can also directly call `consumeWith`, as a shortcut for `source.asObservable.consumeWith(consumer)`, see an example below:

```scala
//given the same `source` as the above example`

//when
val t: Task[List[Int]] = source.consumeWith(Consumer.toList) //`consumeWith` as extended method of `Source`

//then
t.runSyncUnsafe() should contain theSameElementsAs elements
```

#### asConsumer

On the other hand, see how to convert an `Sink[-In, +Out <: Future[Mat]]` into a `Consumer[+In, +Mat]`.
```scala
//given
val foldSumSink: Sink[Int, Future[Int]] = Sink.fold[Int, Int](0)((acc, num) => acc + num)

//when
val consumer: Consumer[Int, Int] = foldSumSink.asConsumer[Int] //`asConsumer` as an extended method of `Sink`

//then
val t: Task[Int] = Observable.fromIterable(Seq(1, 2, 3)).consumeWith(consumer)
t.runSyncUnsafe() should be 6
```

Finally, you can also convert `Flow[-In, +Out, +Mat]` into `Consumer[+In, +Out]` in the same way you did with `Sink`
 in the previous example.

```scala
//given
val foldSumFlow: Flow[Int, Int, NotUsed] = Flow[Int].fold[Int](0)((acc, num) => acc + num)

//when
val (consumer: Consumer[Int, Int]) = foldSumFlow.asConsumer //`asConsumer` as an extended method of `Flow`
val t: Task[Int] = Observable.fromIterable(Seq(1, 2, 3)).consumeWith(consumer)

t.runSyncUnsafe() should be 6
```

Notice that this interoperability would allow the Monix user to take advantage of the already pre built integrations 
from [Alpakka](https://doc.akka.io/docs/alpakka/current/index.html) or any other Akka Streams implementation.

### Akka &larr; Monix

#### asSource
On the other hand, for converting from Monix `Observable[+In]` to Akka Streams `Source[+In, NotUsed]` we would use the conversion signature `asSource`.

```scala
//given
val ob: Observable[Int] = Observable.range(1, 100)

//when
val f: Future[Seq[Long]] = ob.asSource.runWith(Sink.seq) 

//then eventualy will return a sequence from 1 to 100
```

#### asConsumer

Finally, the converter `asSink` is available for converting from `Consumer[-In, +Mat]` to `Sink[-In, +Out <: Future[Mat]]`. 

```scala
//given
val l: List[Int] = List(1, 2, 3)
val headConsumer: Consumer.Sync[Int, Int] = Consumer.head[Int]

//when
val f: Future[Int] = Source(l).runWith(headConsumer.asSink)

//then eventually will materialize to 1 (the head)
```