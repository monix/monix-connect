package monix.connect

import com.sksamuel.elastic4s._
import monix.eval.Task
import monix.execution.internal.InternalApi

package object es {

  @InternalApi private[es] implicit val taskFunctor: Functor[Task] = new Functor[Task] {
    override def map[A, B](fa: Task[A])(f: A => B): Task[B] = fa.map(f)
  }

  @InternalApi private[es] implicit val taskExecutor: Executor[Task] = (client: HttpClient, request: ElasticRequest) =>
    Task.async(k => client.send(request, k))
}
