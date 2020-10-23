package monix.connect

import com.sksamuel.elastic4s.http._
import monix.eval.Task

package object es {

  implicit val taskFunctor: Functor[Task] = new Functor[Task] {
    override def map[A, B](fa: Task[A])(f: A => B): Task[B] = fa.map(f)
  }

  implicit val taskExecutor: Executor[Task] = (client: HttpClient, request: ElasticRequest) =>
    Task.async(k => client.send(request, k))
}
