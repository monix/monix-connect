import cats.effect.Resource

import monix.eval.Task
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import monix.execution.Scheduler.Implicits.global

val resource = Resource.make[Task, String](Task("password"))(str => Task(println(s"Release: $str"))).use(e => Task(println("Process")))

import cats.effect.{IO, Resource}

def mkResource(s: String) = {
  val acquire = Task(println(s"Acquiring $s")) *> Task.pure(s)

  def release(s: String) = Task(println(s"Releasing $s"))

  Resource.make(acquire)(release)
}

val r = for {
  outer <- mkResource("outer")
  inner <- mkResource("inner")
} yield (outer, inner)

r.use { case (a, b) => Task(println(s"Using $a and $b")) }.runSyncUnsafe()


//Await.result(resource.runToFuture, Duration.Inf)
resource.runSyncUnsafe()