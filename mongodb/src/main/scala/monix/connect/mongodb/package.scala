package monix.connect

import monix.connect.mongodb.domain.RetryStrategy
import monix.eval.{Coeval, Task}
import monix.execution.internal.InternalApi
import org.reactivestreams.Publisher

import scala.concurrent.duration.{Duration, FiniteDuration}

package object mongodb {

  /**
    * An internal method used by those operations that wants to implement a retry interface based on
    * a given limit and timeout.
    *
    * @param publisher a reactivestreams publisher that represents the generic mongodb operation.
    * @param retries   the number of times the operation will be retried in case of unexpected failure,
    *                  being zero retries by default.
    * @param timeout   expected timeout that the operation is expected to be executed or else return a failure.
    * @tparam T the type of the publisher
    * @return a [[Task]] with an optional [[T]] or a failed one if the failures exceeded the retries.
    */
  @InternalApi private[mongodb] def retryOnFailure[T](
                                                       publisher: Coeval[Publisher[T]],
                                                       retries: Int,
                                                       timeout: Option[FiniteDuration],
                                                       delayAfterFailure: Option[FiniteDuration]): Task[Option[T]] = {

    require(retries >= 0, "Retries per operation must be higher or equal than 0.")
    val t = Task.fromReactivePublisher(publisher.value())
    timeout
      .map(t.timeout(_))
      .getOrElse(t)
      .redeemWith(
        ex =>
          if (retries > 0) {
            val retry = retryOnFailure(publisher, retries - 1, timeout, delayAfterFailure)
            delayAfterFailure match {
              case Some(delay) => retry.delayExecution(delay)
              case None => retry
            }
          } else Task.raiseError(ex),
        _ match {
          case None =>
            if (retries > 0) retryOnFailure(publisher, retries - 1, timeout, delayAfterFailure)
            else Task(None)
          case some: Some[T] => Task(some)
        }
      )
  }


  /**
    * An internal method used by those operations that wants to implement a retry interface based on
    * a given limit and timeout.
    *
    * @param publisher a reactivestreams publisher that represents the generic mongodb operation.
    * @tparam T the type of the publisher
    * @return a [[Task]] with an optional [[T]] or a failed one if the failures exceeded the retries.
    */
  @InternalApi private[mongodb] def retryOnFailure[T](publisher: => Publisher[T],
                                                      retryStrategy: RetryStrategy): Task[Option[T]] = {
    require(retryStrategy.retries >= 0, "Retries per operation must be higher or equal than 0.")
    Task.fromReactivePublisher(publisher)
      .redeemWith(
        ex =>
          if (retryStrategy.retries > 0) {
            val retry = retryOnFailure(publisher, retryStrategy.copy(retries = retryStrategy.retries -1))
            retryStrategy.backoffDelay match {
              case Duration.Zero => retry
              case delay => retry.delayExecution(delay)
            }
          } else Task.raiseError(ex),
        _ match {
          case None =>
            if (retryStrategy.retries > 0) retryOnFailure(publisher, retryStrategy.copy(retries = retryStrategy.retries -1))
            else Task.now(None)
          case some: Some[T] => Task.now(some)
        }
      )
  }

}

