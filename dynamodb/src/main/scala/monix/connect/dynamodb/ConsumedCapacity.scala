package monix.connect.dynamodb

object ConsumedCapacity extends Enumeration {
  type Detail = Value
  val INDEXES, TOTAL, NONE = Value
}
