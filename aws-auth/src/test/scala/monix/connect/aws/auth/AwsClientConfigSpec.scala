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

import java.net.URI

import org.scalatest.flatspec.AnyFlatSpec
import pureconfig.ConfigSource
import pureconfig._
import pureconfig.generic.auto._
import MonixAwsConf.Implicits._
import org.scalatest.matchers.should.Matchers
import pureconfig.error.ConfigReaderException
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region

import scala.util.{Failure, Try}

class AwsClientConfigSpec extends AnyFlatSpec with Matchers {

  s"${AppConf}" should "load from default config file" in {
    //given/when
    val awsClientConf = AppConf.loadOrThrow

    //then
    awsClientConf.monixAws.credentials shouldBe a[DefaultCredentialsProvider]
    awsClientConf.monixAws.endpoint.isDefined shouldBe false //Some(URI.create("localhost:4566"))
    awsClientConf.monixAws.region shouldBe Region.AWS_GLOBAL
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
    val awsClientConf = configSource.loadOrThrow[AppConf]

    //then
    awsClientConf.monixAws.credentials shouldBe a[DefaultCredentialsProvider]
    awsClientConf.monixAws.endpoint shouldBe Some(URI.create("localhost:4566"))
    awsClientConf.monixAws.httpClient.isDefined shouldBe false
    awsClientConf.monixAws.region shouldBe Region.AWS_GLOBAL
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
    val awsClientConf = configSource.loadOrThrow[AppConf]

    //then
    awsClientConf.monixAws.credentials shouldBe a[DefaultCredentialsProvider]
    awsClientConf.monixAws.endpoint.isDefined shouldBe false
    awsClientConf.monixAws.httpClient.isDefined shouldBe false
    awsClientConf.monixAws.region shouldBe Region.AWS_GLOBAL
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
    val awsClientConf = Try(configSource.loadOrThrow[AppConf])

    //then
    awsClientConf.isFailure shouldBe true
    awsClientConf shouldBe a[Failure[ConfigReaderException[AppConf]]]
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
    val awsClientConf = Try(configSource.loadOrThrow[AppConf])

    //then
    awsClientConf.isFailure shouldBe true
    awsClientConf shouldBe a[Failure[ConfigReaderException[AppConf]]]
  }

}
