package monix.connect.mongodb

import com.mongodb.client.model.{
  CountOptions,
  DeleteOptions,
  FindOneAndDeleteOptions,
  FindOneAndReplaceOptions,
  FindOneAndUpdateOptions,
  InsertManyOptions,
  InsertOneOptions,
  ReplaceOptions,
  UpdateOptions
}
import monix.execution.internal.InternalApi

package object domain {

  // default options
  @InternalApi private[mongodb] val DefaultDeleteOptions = new DeleteOptions()
  @InternalApi private[mongodb] val DefaultCountOptions = new CountOptions()
  @InternalApi private[mongodb] val DefaultFindOneAndDeleteOptions = new FindOneAndDeleteOptions()
  @InternalApi private[mongodb] val DefaultFindOneAndReplaceOptions = new FindOneAndReplaceOptions()
  @InternalApi private[mongodb] val DefaultFindOneAndUpdateOptions = new FindOneAndUpdateOptions()
  @InternalApi private[mongodb] val DefaultInsertOneOptions = new InsertOneOptions()
  @InternalApi private[mongodb] val DefaultInsertManyOptions = new InsertManyOptions()
  @InternalApi private[mongodb] val DefaultUpdateOptions = new UpdateOptions()
  @InternalApi private[mongodb] val DefaultReplaceOptions = new ReplaceOptions()

  // results
  @InternalApi private[mongodb] case class DeleteResult(deleteCount: Long, wasAcknowledged: Boolean)
  @InternalApi private[mongodb] case class InsertOneResult(insertedId: Option[String], wasAcknowledged: Boolean)
  @InternalApi private[mongodb] case class InsertManyResult(insertedIds: Set[String], wasAcknowledged: Boolean)
  @InternalApi private[mongodb] case class UpdateResult(
    matchedCount: Long,
    modifiedCount: Long,
    wasAcknowledged: Boolean)

  // default results
  @InternalApi private[mongodb] val DefaultDeleteResult = DeleteResult(deleteCount = 0L, wasAcknowledged = false)
  @InternalApi private[mongodb] val DefaultInsertOneResult =
    InsertOneResult(insertedId = Option.empty[String], wasAcknowledged = false)
  @InternalApi private[mongodb] val DefaultInsertManyResult =
    InsertManyResult(insertedIds = Set.empty[String], wasAcknowledged = false)
  @InternalApi private[mongodb] val DefaultUpdateResult =
    UpdateResult(matchedCount = 0L, modifiedCount = 0L, wasAcknowledged = false)
}
