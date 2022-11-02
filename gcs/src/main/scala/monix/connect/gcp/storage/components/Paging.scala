/*
 * Copyright (c) 2020-2021 by The Monix Connect Project Developers.
 * See the project homepage at: https://connect.monix.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package monix.connect.gcp.storage.components

import com.google.api.gax.paging.Page
import monix.eval.Task
import monix.reactive.Observable

import scala.jdk.CollectionConverters._

private[storage] trait Paging {

  protected def walk[A](f: Task[Page[A]]): Observable[A] = {

    def next(page: Page[A]): Task[(Page[A], Page[A])] = {
      if (!page.hasNextPage) Task.now((page, page))
      else {
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
