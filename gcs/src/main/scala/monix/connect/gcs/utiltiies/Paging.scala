package monix.connect.gcs.utiltiies

import com.google.api.gax.paging.Page
import monix.eval.Task
import monix.reactive.Observable

import scala.jdk.CollectionConverters._

trait Paging {

  protected def walk[A](f: Task[Page[A]]): Observable[A] = {
    def next(page: Page[A]): Task[(Page[A], Page[A])] = {
      if (!page.hasNextPage) Task.now((page, page)) else {
        Task.evalAsync(page.getNextPage).map(next => (page, next))
      }
    }

    Observable
      .fromTask(f.executeAsync)
      .flatMap(Observable.fromAsyncStateAction(next)(_))
      .takeWhileInclusive(_.hasNextPage)
      .concatMapIterable(_.iterateAll().asScala.toList)
  }
}
