---
id: akka
title: Akka Streams
---

## Introduction
  
This module makes interoperability with akka streams easier by simply defining implicit extended classes for reactive stream conversions between akka and monix.

The three main core abstractions defined in akka streams are _Source_, _Flow_ and _Sink_, 
they were designed following the JVM [reactive streams](https://github.com/reactive-streams/reactive-streams-jvm/blob/v1.0.3/README.md) standards, 
meaning that under it's design they implement the `Publisher` and `Subscriber` contract. 
Which at the end it means that they can interoperate with other libraries that also implements the reactive stream specification, such as [Monix Reactive](https://monix.io/api/3.2/monix/reactive/index.html).
 So this module aims to provide a nice interoperability between these two reactive streams libraries.
 
 In order to achieve it, this module will provide an extended conversion method for each of the stream abstractions mentioned before. 
  These implicit extended methods can be imported from: `monix.connect.akka.stream.Converters._`.
Therefore, under the scope of the import, the signatures `.asObservable` and `.asConsumer` will be available for the `Source`, `Flow`, and `Sink`.

The below table shows in more detail the specs for the conversion from akka stremas to monix:  

  | _Akka_ | _Monix_ | _Akka &rarr; Monix_ | _Akka &larr; Monix_ |
  | :---: | :---: | :---: | :---: | 
  | _Source[+In, +Mat]_ | _Observable[+In]_ | `source.asObservable[In]` | `observable.asSource[In]` |
  | _Flow[-In, +Out, +Mat]_ | _Consumer[-In, +Out]_ | `flow.asConsumer[Out]` | - |
  | _Sink[-In, +Out <: Future[Mat]]_ | _Consumer[-In, +Mat]_ | `sink.asConsumer[Mat]` | `consumer.asSink[In]` |

Note that two methods does not need to be typed as it has been done explicitly in the example table, the compiler will infer it for you.

Also, in order to perform these conversion it is required to have an implicit instance of `akka.stream.Materializer` and `monix.execution.Scheduler` in the scope.

## Dependency

Add the following dependency to get started:
```scala 
libraryDependencies += "io.monix" %% "monix-akka" % "0.1.0"
```

## Getting started

The following code shows how these implicits can be initialized, but `Scheduler` and `ActorSystem` can be initialized differently and configured differently depending on the use case.
```scala
import monix.connect.akka.stream.Converters._
import monix.execution.Scheduler.Implicits.global

val actorSystem: ActorSystem = ActorSystem("Akka-Streams-InterOp") 
implicit val materializer = ActorMaterializer() 
```

### Akka &rarr; Monix

#### asObservable

Let's see an example for converting an `Source[+In, +Mat]` to `Observable[+In]`:

```scala
//given
val elements = 1 until 50

//when
val ob: Observable[Int] = Source.from(elements).asObservable //`asObservable` converter as extended method of source.

//then 
ob.toListL.runSyncUnsafe() should contain theSameElementsAs elements
```

In this case we have not needed to consume the `Observable` since we directly used an operator that collects 
to a list `.toList`, but note that in case you need to use an specific consumer, you can also directly call `consumeWith`, as a shortcut for `source.asObservable.consumeWith(consumer)`, see an example below:

```scala
//given the same `elements` and `source` as above example`

//then
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
val (consumer: Consumer[Int, Int]) = foldSumSink.asConsumer[Int] //`asConsumer` as an extended method of `Sink`
val t: Task[Int] = Observable.fromIterable(Seq(1, 2, 3)).consumeWith(consumer)

//then the future value of `t` should be 6
```

Finally, you can also convert `Flow[-In, +Out, +Mat]` into `Consumer[+In, +Out]` in the same way you did with `Sink`
 in the previous example.

```scala
//given
val foldSumFlow: Flow[Int, Int, NotUsed] = Flow[Int].fold[Int](0)((acc, num) => acc + num)

//when
val (consumer: Consumer[Int, Int]) = foldSumFlow.asConsumer //`asConsumer` as an extended method of `Flow`
val t: Task[Int] = Observable.fromIterable(Seq(1, 2, 3)).consumeWith(consumer)

//then the future value of `t` should be 6
```

Notice that this interoperability would allow the Monix user to take advantage of the already pre built integrations 
from [Alpakka](https://doc.akka.io/docs/alpakka/current/index.html) or any other Akka Streams implementation.

### Akka &larr; Monix

#### asSource
On the other hand, for converting from Monix `Observable[+In]` to Akka Streams `Source[+In, NotUsed]` we would use the conversion signature `asSource`.

```scala
import monix.connect.akka.stream.Converters._
val f: Future[Seq[Long]] = Observable.range(1, 100).asSource.runWith(Sink.seq) 
//eventualy will return a sequence from 1 to 100
```

#### asConsumer

Finally, for converting from `Sink[-In, +Out <: Future[Mat]]` to `Consumer[-In, +Mat]`. 

```scala
//given
import monix.connect.akka.stream.Converters._
val sink: Sink[Int, Future[String]] = Sink.fold[String, Int]("")((s, i) => s + i.toString)
val t: Task[String] = Observable.fromIterable(1 until 10).consumeWith(sink.asConsumer[String])
//eventually will return "123456789"
```