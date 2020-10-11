package monix.connect.aws.auth

import scala.concurrent.duration.FiniteDuration

//todo add proxy, ssl and tls settings
case class HttpClientConf(maxConcurrency: Option[Int],
                          maxPendingConnectionAcquires: Option[Int],
                          connectionAcquisitionTimeout: Option[FiniteDuration],
                          connectionMaxIdleTime: Option[FiniteDuration],
                          connectionTimeToLive: Option[FiniteDuration],
                          useIdleConnectionReaper: Boolean,
                          readTimeout: Option[FiniteDuration],
                          writeTimeout: Option[FiniteDuration])
