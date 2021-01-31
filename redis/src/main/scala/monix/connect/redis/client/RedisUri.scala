package monix.connect.redis.client

import io.lettuce.core.RedisURI

import java.time.Duration
import scala.concurrent.duration.FiniteDuration

case class RedisUri private[redis] (host: String, port: Int, database: Option[Int] = None,
                     password: Option[String] = None, ssl: Option[Boolean] = None, verifyPeer: Option[Boolean] = None, startTls: Option[Boolean] = None,
                     timeout: Option[FiniteDuration], sentinels: List[String] = List.empty,
                     socket: Option[String] = None, sentinelMasterId: Option[String] = None,
                     clientName: Option[String] = None) {

  def withDatabase(database: Int): RedisUri = copy(database = Some(database))
  def withPassword(password: String): RedisUri = copy(password = Some(password))
  def withSsl(ssl: Boolean): RedisUri = copy(ssl = Some(ssl))
  def withVerifyPeer(verifyPeer: Boolean): RedisUri = copy(verifyPeer = Some(verifyPeer))
  def withStartTls(startTls: Boolean): RedisUri = copy(startTls = Some(startTls))
  def withTimeout(timeout: FiniteDuration): RedisUri = copy(timeout = Some(timeout))
  def withSentinels(sentinels: List[String]): RedisUri = copy(sentinels = sentinels)
  def withSocket(socket: String): RedisUri = copy(socket = Some(socket))
  def withSentinelMasterId(sentinelMasterId: String): RedisUri = copy(sentinelMasterId = Some(sentinelMasterId))
  def withClientName(clientName: String): RedisUri = copy(clientName = Some(clientName))

  private[redis] def toJava: RedisURI = {
    val builder = RedisURI.builder().withHost(host).withPort(port)
    database.map(builder.withDatabase)
    password.map(builder.withPassword)
    ssl.map(builder.withSsl)
    verifyPeer.map(builder.withVerifyPeer)
    startTls.map(builder.withStartTls)
    timeout.map(timeout => builder.withTimeout(Duration.ofMillis(timeout.toMillis)))
    sentinels.map(builder.withSentinel) //todo add sentinel host and port
    //socket.map(builder.socket) // todo figure out why it does not compile
    sentinelMasterId.map(builder.withSentinelMasterId)
    clientName.map(builder.withClientName)
    builder.build()
  }
}


object RedisUri {

  def apply(host: String, port: Int): RedisUri = RedisUri(host, port)

}