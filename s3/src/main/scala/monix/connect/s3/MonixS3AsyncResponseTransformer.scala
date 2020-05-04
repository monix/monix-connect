/*
 * Copyright (c) 2014-2020 by The Monix Connect Project Developers.
 * See the project homepage at: https://monix.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package monix.connect.s3

import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture

import monix.eval.Task
import monix.reactive.Observable
import software.amazon.awssdk.core.async.{AsyncResponseTransformer, SdkPublisher}
import software.amazon.awssdk.services.s3.model.GetObjectResponse

/**
  * Transformer implementation from aws sdk publisher to a completable future
  * which is required by the signature [[S3.getObject()]]
  */
private[s3] class MonixS3AsyncResponseTransformer
  extends AsyncResponseTransformer[GetObjectResponse, Task[ByteBuffer]] {
  val future = new CompletableFuture[Task[ByteBuffer]]()

  override def prepare(): CompletableFuture[Task[ByteBuffer]] = future

  override def onResponse(response: GetObjectResponse): Unit = ()

  override def onStream(publisher: SdkPublisher[ByteBuffer]): Unit = {
    future.complete(
      Observable
        .fromReactivePublisher(publisher)
        .foldLeftL(Array.emptyByteArray){
          (buffer, chunk) => buffer ++ chunk.array()
        }.map(ByteBuffer.wrap(_)))
    ()
  }
  override def exceptionOccurred(error: Throwable): Unit = {
    future.completeExceptionally(error)
    ()
  }
}
