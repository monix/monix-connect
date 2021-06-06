import monix.eval.Task
import monix.execution.exceptions.DummyException
import monix.reactive.Observable
import org.scalacheck.Gen
import monix.execution.Scheduler.Implicits.global

val genNum = Gen.choose(1, 5)

val t = Task{genNum}.map(_.sample.get).flatMap(n => if(n == 2) {
  println("Got a failure")
  Task.raiseError(DummyException("Dummy exception"))
} else { Task.now(n)})

val ob = Observable.repeatEvalF(t).map(n => println(s"N: ${n}")).onErrorRestart(3).take(20).toListL.runSyncUnsafe()

println("Obs: " + ob.mkString(", "))