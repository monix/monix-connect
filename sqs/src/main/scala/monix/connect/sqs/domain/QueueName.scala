package monix.connect.sqs.domain

/**
  * Simple case class encodes the queueUrl.
  * The reason for this class to exist is to have
  * a proper distinction between the queue name and url,
  * so that they can not be confused on the method signatures.
  */
final case class QueueName(name: String) {
  def map[A, B](f: String => String): QueueName = QueueName(f(name))
}




