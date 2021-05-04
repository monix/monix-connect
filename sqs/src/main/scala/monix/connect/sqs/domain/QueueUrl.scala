package monix.connect.sqs.domain

/**
  * Simple case class encodes the queueUrl.
  * The reason for this class to exist is to have
  * a proper distinction between the queue url and name,
  * so that they can not be confused on the method signatures.
  */
final case class QueueUrl(url: String)
