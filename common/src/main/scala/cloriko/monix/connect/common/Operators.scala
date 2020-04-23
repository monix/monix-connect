package cloriko.monix.connect.common

import monix.reactive.Observable

object Operators {

  type Transformer[A, B] = Observable[A] => Observable[B]

  object Implicits {
    implicit class ObservableExtension[A](ob: Observable[A]) {
      def transform[B](transformer: Transformer[A, B]): Observable[B] = {
        transformer(ob)
      }
    }
  }

}
