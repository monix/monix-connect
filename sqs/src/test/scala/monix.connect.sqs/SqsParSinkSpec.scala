/*
 * Copyright (c) 2020-2021 by The Monix Connect Project Developers.
 * See the project homepage at: https://connect.monix.io
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

import monix.connect.sqs.domain.QueueUrl
import monix.connect.sqs.inbound.{FifoMessage, SqsParBatchSink}
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class SqsParSinkSpec extends AnyFlatSpecLike with Matchers {

  def genFifoMessage(groupId: String = "groupId123", deduplicationId: Option[String] = None): Gen[FifoMessage] =
    Gen.identifier.map(_.take(10)).map(id => FifoMessage(id, groupId = groupId, deduplicationId = deduplicationId))

  "A single inbound message" can "be send as if it was a batch" in {
    val message = genFifoMessage().sample.get
    val batches = SqsParBatchSink.groupMessagesInBatches(List(message), QueueUrl(""))
    batches.size shouldBe 1
    batches.flatten(_.entries().asScala).size shouldBe 1
  }

  "Ten inbound messages" must "be grouped in a single batch" in {
    val messages = Gen.listOfN(10, genFifoMessage()).sample.get
    val batches = SqsParBatchSink.groupMessagesInBatches(messages, QueueUrl(""))
    batches.size shouldBe 1
    batches.flatten(_.entries().asScala).size shouldBe 10
  }

  "More than ten messages" must "be grouped in a single batch" in {
    val messages = Gen.listOfN(11, genFifoMessage()).sample.get
    val batches = SqsParBatchSink.groupMessagesInBatches(messages, QueueUrl(""))
    batches.size shouldBe 2
    batches.flatten(_.entries().asScala).size shouldBe 11
  }

  "N messages" must "be grouped in a single batch" in {
    val n = Gen.choose(21, 1000).sample.get
    val messages = Gen.listOfN(n, genFifoMessage()).sample.get
    val batches = SqsParBatchSink.groupMessagesInBatches(messages, QueueUrl(""))
    batches.size shouldBe (n / 10) + (if((n % 10)==0) 0 else 1)
    batches.flatten(_.entries().asScala).size shouldBe messages.size
  }

}
