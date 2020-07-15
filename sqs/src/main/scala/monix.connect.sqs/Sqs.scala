/*
 * Copyright (c) 2020-2020 by The Monix Connect Project Developers.
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

package monix.connect.sqs

import monix.eval.Task
import monix.reactive.{Consumer, Observable}
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{Message, ReceiveMessageRequest, SqsRequest, SqsResponse}

import scala.jdk.CollectionConverters._

object Sqs {

  //todo add settings for batch size, timeout, attributeNames and messageAttributes
  def source(queueUrl: String)
            (implicit client: SqsAsyncClient): Observable[Message] = {
    for {
      r <- Observable.repeat[ReceiveMessageRequest] {
        ReceiveMessageRequest.builder.queueUrl(queueUrl).build()
      }
      l <- Observable.fromTask {
        Task.from(client.receiveMessage(r)).map(_.messages().asScala.toList)
      }
      m <- Observable.suspend(Observable.fromIterable(l))
    } yield m
  }

  def sink[In <: SqsRequest, Out <: SqsResponse](
    implicit
    sqsOp: SqsOp[In, Out],
    client: SqsAsyncClient): Consumer[In, Out] = new SqsSink

  def transformer[In <: SqsRequest, Out <: SqsResponse](
    implicit
    sqsOp: SqsOp[In, Out],
    client: SqsAsyncClient): Observable[In] => Observable[Task[Out]] = { inObservable: Observable[In] =>
    inObservable.map(in => Task.from(sqsOp.execute(in)))
  }


}
