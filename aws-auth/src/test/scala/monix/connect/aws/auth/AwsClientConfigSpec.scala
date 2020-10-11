/*
 * Copyright (c) 2020-2020 by The Monix Connect Project Developers.
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

package monix.connect.aws.auth

import org.scalatest.flatspec.AnyFlatSpec
import pureconfig.ConfigSource
import pureconfig._
import pureconfig.generic.auto._
import AwsClientConf._
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region

class AwsClientConfigSpec extends AnyFlatSpec with Matchers {

  s"${AwsClientConf}" should "load from default config file" in {
    //given/when
    val awsClientConf = AwsClientConf.loadOrThrow

    //then
    awsClientConf.credentials shouldBe a[DefaultCredentialsProvider]
    awsClientConf.endpoint.isDefined shouldBe true
    awsClientConf.httpClient.isDefined shouldBe true
    awsClientConf.region shouldBe Region.AWS_GLOBAL
  }

  it should "not require endpoint nor http client settings" in {
    //given
    val configSource = ConfigSource.string(
      "" +
        s"""
           |{
           |  credentials: {
           |    provider: "default"
           |  }
           |  region: "aws-global"
           |}
           |""".stripMargin)

    //when
    val awsClientConf = configSource.loadOrThrow[AwsClientConf]

    //then
    awsClientConf.credentials shouldBe a[DefaultCredentialsProvider]
    awsClientConf.endpoint.isDefined shouldBe false
    awsClientConf.httpClient.isDefined shouldBe false
    awsClientConf.region shouldBe Region.AWS_GLOBAL
  }

}
