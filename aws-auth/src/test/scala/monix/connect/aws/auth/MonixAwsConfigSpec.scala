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
import pureconfig.{CamelCase, ConfigFieldMapping, ConfigSource, KebabCase, PascalCase, SnakeCase}
import pureconfig.generic.auto._
import org.scalatest.matchers.should.Matchers
import pureconfig.error.ConfigReaderException
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region
import monix.connect.aws.auth.MonixAwsConf._
import monix.execution.Scheduler.Implicits.global
import pureconfig.generic.ProductHint

import java.io.File
import scala.util.Try

class MonixAwsConfigSpec extends AnyFlatSpec with Matchers {

  "MonixAwsConf" should "load from default config file" in {
    //given/when
    val monixAwsConf = MonixAwsConf.load().runSyncUnsafe()

    //then
    monixAwsConf.credentials shouldBe a[DefaultCredentialsProvider]
    monixAwsConf.endpoint.isDefined shouldBe true
    monixAwsConf.region shouldBe Region.EU_WEST_1
  }

  it should "read the local endpoint as a uri" in {

    //given
    implicit val hint: ProductHint[AppConf] =
      ProductHint(ConfigFieldMapping(CamelCase, KebabCase), useDefaultArgs = false, allowUnknownKeys = true)

    val configSource = ConfigSource.string(
      "" +
        s"""
           |monix-aws: {
           |  credentials: {
           |           provider: "default"
           |   }
           |    region: "aws-global"
           |    endpoint: "localhost:4566"
           |}
           |""".stripMargin)

    //when
    val monixAwsConf = configSource.loadOrThrow[AppConf].monixAws

    //then
    monixAwsConf.credentials shouldBe a[DefaultCredentialsProvider]
    monixAwsConf.endpoint shouldBe Some(URI.create("localhost:4566"))
    monixAwsConf.httpClient.isDefined shouldBe false
    monixAwsConf.region shouldBe Region.AWS_GLOBAL
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
           |  }
           |}
           |""".stripMargin)

    //when
    val monixAwsConf = configSource.loadOrThrow[AppConf].monixAws

    //then
    monixAwsConf.credentials shouldBe a[DefaultCredentialsProvider]
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

  it can "read config in kebabCase" in {
    //given/when
    val monixAwsConf =
      MonixAwsConf.file(new File("aws-auth/src/test/resources/kebab-case.conf"), KebabCase).runSyncUnsafe()

    //then
    monixAwsConf.credentials shouldBe a[DefaultCredentialsProvider]
    monixAwsConf.endpoint shouldBe Some(URI.create("kebab-case:12345"))
    monixAwsConf.region shouldBe Region.EU_WEST_1
  }

  it can "read config in snake_case" in {
    //given/when
    val monixAwsConf =
      MonixAwsConf.file(new File("aws-auth/src/test/resources/snake_case.conf"), SnakeCase).runSyncUnsafe()

    //then
    monixAwsConf.credentials shouldBe a[DefaultCredentialsProvider]
    monixAwsConf.endpoint shouldBe Some(URI.create("snake:12345"))
    monixAwsConf.region shouldBe Region.EU_WEST_1
  }

  it can "read config in PascalCase" in {
    //given/when
    val monixAwsConf =
      MonixAwsConf.file(new File("aws-auth/src/test/resources/PascalCase.conf"), PascalCase).runSyncUnsafe()

    //then
    monixAwsConf.credentials shouldBe a[DefaultCredentialsProvider]
    monixAwsConf.endpoint shouldBe Some(URI.create("PascalCase:12345"))
    monixAwsConf.region shouldBe Region.EU_WEST_1
  }

  it can "read config in camelCase" in {
    //given/when
    val monixAwsConf =
      MonixAwsConf.file(new File("aws-auth/src/test/resources/camelCase.conf"), CamelCase).runSyncUnsafe()

    //then
    monixAwsConf.credentials shouldBe a[DefaultCredentialsProvider]
    monixAwsConf.endpoint shouldBe Some(URI.create("camelCase:12345"))
    monixAwsConf.region shouldBe Region.EU_WEST_1
  }

  it should "read config in kebab-case by default from reference.conf " in {
    //given/when
    val monixAwsConf = MonixAwsConf.load().runSyncUnsafe()

    //then
    monixAwsConf.credentials shouldBe a[DefaultCredentialsProvider]
    monixAwsConf.endpoint shouldBe Some(URI.create("localhost:4566"))
    monixAwsConf.region shouldBe Region.EU_WEST_1
  }

  it should "read config in camelCase by default when reading from path" in {
    //given/when
    val monixAwsConf = MonixAwsConf.file(new File("aws-auth/src/test/resources/kebab-case.conf")).runSyncUnsafe()

    //then
    monixAwsConf.credentials shouldBe a[DefaultCredentialsProvider]
    monixAwsConf.endpoint shouldBe Some(URI.create("kebab-case:12345"))
    monixAwsConf.region shouldBe Region.EU_WEST_1
  }

}
