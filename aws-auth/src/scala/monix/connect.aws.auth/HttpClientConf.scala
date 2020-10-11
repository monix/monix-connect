package monix.connect.aws.auth

import monix.execution.internal.InternalApi

import scala.concurrent.duration.FiniteDuration

@InternalApi
private[connect] case class HttpClientConf(maxConcurrency: Option[Int],
                          maxPendingConnectionAcquires: Option[Int],
                          connectionAcquisitionTimeout: Option[FiniteDuration],
                          connectionMaxIdleTime: Option[FiniteDuration],
                          connectionTimeToLive: Option[FiniteDuration],
                          useIdleConnectionReaper: Boolean,
                          readTimeout: Option[FiniteDuration],
                          writeTimeout: Option[FiniteDuration])
