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

import java.io.{BufferedInputStream, BufferedOutputStream, FileInputStream, FileOutputStream}
import java.nio.file.Path

import cats.effect.ExitCase
import monix.eval.Task
import monix.reactive.Observable
import cats.effect.Resource

private[storage] trait FileIO {

  protected def openFileInputStream(path: Path): Resource[Task, BufferedInputStream] = {
    Resource.make[Task, BufferedInputStream] {
      Task(new BufferedInputStream(new FileInputStream(path.toFile)))
    } { fis => Task(fis.close()) }
  }

  protected def openFileOutputStream(path: Path): Observable[BufferedOutputStream] = {
    Observable.resourceCase {
      Task(new BufferedOutputStream(new FileOutputStream(path.toFile)))
    } {
      case (fos, ExitCase.Completed) =>
        for {
          _ <- Task(fos.flush())
          _ <- Task(fos.close())
        } yield ()

      case (fos, _) =>
        Task(fos.close())
    }
  }
}
