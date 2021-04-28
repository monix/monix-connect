package monix.connect.sqs.domain

case class QueueName(name: String) {
  def map[A, B](f: String => String): QueueName = QueueName(f(name))
}

case class QueueUrl(url: String)

object QueueRef {

  def fromName(name: String): QueueName = QueueName(name)

  def fromUrl(name: String): QueueUrl = QueueUrl(name)

}


