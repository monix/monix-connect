package monix.connect.sqs.domain

final case class QueueName(name: String) {
  def map[A, B](f: String => String): QueueName = QueueName(f(name))
}




