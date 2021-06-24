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

package monix.connect.aws.auth

import java.net.URI
import org.scalatest.flatspec.AnyFlatSpec
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import monix.connect.aws.auth.MonixAwsConf.AppConf
import org.scalatest.matchers.should.Matchers
import pureconfig.error.ConfigReaderException
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region
import monix.connect.aws.auth.MonixAwsConf._
import scala.util.Try

class MonixAwsConfigSpec extends AnyFlatSpec with Matchers {

  "MonixAwsConf" should "load from default config file" in {
    //given/when
    import monix.execution.Scheduler.Implicits.global
    val monixAwsConf = MonixAwsConf.load.runSyncUnsafe()

    //then
    monixAwsConf.credentialsProvider shouldBe a[DefaultCredentialsProvider]
    monixAwsConf.endpoint.isDefined shouldBe false
    monixAwsConf.region shouldBe Region.EU_WEST_1
  }

  it should "not require endpoint nor http client settings" in {
    //given
    val configSource = ConfigSource.string(
      "" +
        s"""
           |{
           |  monix-aws: {
           |    credentials: {
           |      provider: "default"
           |    }
           |    region: "aws-global"
           |    endpoint: "localhost:4566"
           |  }
           |}
           |""".stripMargin)

    //when
    val monixAwsConf = configSource.loadOrThrow[AppConf].monixAws

    //then
    monixAwsConf.credentialsProvider shouldBe a[DefaultCredentialsProvider]
    monixAwsConf.endpoint shouldBe Some(URI.create("localhost:4566"))
    monixAwsConf.httpClient.isDefined shouldBe false
    monixAwsConf.region shouldBe Region.AWS_GLOBAL
  }

  it should "read the local endpoint as a uri" in {
    //given
    val configSource = ConfigSource.string(
      "" +
        s"""
           |{
           |  monix-aws: {
           |    credentials: {
           |      provider: "default"
           |    }
           |    region: "aws-global"
           |  }
           |}
           |""".stripMargin)

    //when
    val monixAwsConf = configSource.loadOrThrow[AppConf].monixAws

    //then
    monixAwsConf.credentialsProvider shouldBe a[DefaultCredentialsProvider]
    monixAwsConf.endpoint.isDefined shouldBe false
    monixAwsConf.httpClient.isDefined shouldBe false
    monixAwsConf.region shouldBe Region.AWS_GLOBAL
  }

  it should "fail when credentials are not present" in {
    //given
    val configSource = ConfigSource.string(
      "" +
        s"""
           |{
           |  monix-aws: {
           |    region: "aws-global"
           |  }
           |}
           |""".stripMargin)

    //when
    val monixAwsConf = Try(configSource.loadOrThrow[AppConf]).map(_.monixAws)

    //then
    monixAwsConf.isFailure shouldBe true
    monixAwsConf.failed.get shouldBe a[ConfigReaderException[_]]
    monixAwsConf.failed.get.getMessage should include("Key not found: 'credentials'")
  }

  it should "fail when credentials region is not present" in {
    //given
    val configSource = ConfigSource.string(
      "" +
        s"""
           |{
           |  monix-aws: {
           |    credentials: {
           |      provider: "default"
           |    }
           |  }
           |}
           |""".stripMargin)

    //when
    val monixAwsConf = Try(configSource.loadOrThrow[AppConf]).map(_.monixAws)

    //then
    monixAwsConf.isFailure shouldBe true
    monixAwsConf.failed.get shouldBe a[ConfigReaderException[_]]
    monixAwsConf.failed.get.getMessage should include("Key not found: 'region'")
  }

}
