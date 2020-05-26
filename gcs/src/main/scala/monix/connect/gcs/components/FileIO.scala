package monix.connect.gcs.components

import java.io.{BufferedInputStream, BufferedOutputStream, FileInputStream, FileOutputStream}
import java.nio.file.Path

import cats.effect.ExitCase
import monix.eval.Task
import monix.reactive.Observable

trait FileIO {

  protected def openFileInputStream(path: Path): Observable[BufferedInputStream] = {
    Observable.resource {
      Task(new BufferedInputStream(new FileInputStream(path.toFile)))
    } { fis =>
      Task(fis.close())
    }
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
