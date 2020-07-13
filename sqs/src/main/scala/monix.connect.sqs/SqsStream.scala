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

import monix.reactive.Observable
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{Message, ReceiveMessageRequest}

import scala.jdk.CollectionConverters._

object SqsStream {
  def apply(implicit client: SqsAsyncClient, queueUrl: String): Observable[Message] = {
    val builder = ReceiveMessageRequest.builder.queueUrl(queueUrl)
    val request: ReceiveMessageRequest = builder.build()

    client
      .receiveMessage(request)
      .whenComplete((res, _) => res.messages().asScala.toList.map(println))
      .thenAccept(println)

    client.receiveMessage(request).handle[List[Message]] { (res, _) =>
      println("startedddd")
      println(res.messages())
      res.messages.asScala.toList
    }

    Observable
      .from(
        client.receiveMessage(request).handle[List[Message]] { (res, err) =>
          err match {
            case null => {
              res.messages.asScala.toList
            }
          }
        }
      )
      .flatMap(Observable.fromIterable(_))
  }
}
