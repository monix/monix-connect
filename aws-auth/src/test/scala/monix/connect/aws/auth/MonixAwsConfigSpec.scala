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
import org.scalatest.flatspec.AsyncFlatSpec
import pureconfig.{CamelCase, ConfigSource, KebabCase, PascalCase, SnakeCase}
import org.scalatest.matchers.should.Matchers
import pureconfig.error.ConfigReaderException
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region
import monix.connect.aws.auth.MonixAwsConf._
import monix.connect.aws.auth.configreader.KebabConfigReader
import monix.eval.Task
import monix.execution.Scheduler
import monix.testing.scalatest.MonixTaskTest

import java.io.File
import scala.util.Try

class MonixAwsConfigSpec extends AsyncFlatSpec with MonixTaskTest with Matchers {

  override implicit val scheduler: Scheduler = Scheduler.io("monix-aws-config-spec")

  "MonixAwsConf" should "load from default config file" in {
    MonixAwsConf.load().asserting { monixAwsConf =>
      monixAwsConf.credentials shouldBe a[DefaultCredentialsProvider]
      monixAwsConf.endpoint.isDefined shouldBe true
      monixAwsConf.region shouldBe Region.EU_WEST_1
    }
  }

  it should "read the local endpoint as a uri" in {

    import KebabConfigReader._
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

    Task(configSource.loadOrThrow[AppConf].monixAws).asserting { monixAwsConf =>
      monixAwsConf.credentials shouldBe a[DefaultCredentialsProvider]
      monixAwsConf.endpoint shouldBe Some(URI.create("localhost:4566"))
      monixAwsConf.httpClient.isDefined shouldBe false
      monixAwsConf.region shouldBe Region.AWS_GLOBAL
    }
  }

  it should "not require endpoint nor http client settings" in {
    import KebabConfigReader._
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

    Task(configSource.loadOrThrow[AppConf].monixAws).asserting { monixAwsConf =>
      monixAwsConf.credentials shouldBe a[DefaultCredentialsProvider]
      monixAwsConf.endpoint.isDefined shouldBe false
      monixAwsConf.httpClient.isDefined shouldBe false
      monixAwsConf.region shouldBe Region.AWS_GLOBAL
    }
  }

  it should "fail when credentials are not present" in {
    import KebabConfigReader._

    val configSource = ConfigSource.string(
      "" +
        s"""
           |{
           |  monix-aws: {
           |    region: "aws-global"
           |  }
           |}
           |""".stripMargin)

    val monixAwsConf = Try(configSource.loadOrThrow[AppConf]).map(_.monixAws)

    monixAwsConf.isFailure shouldBe true
    monixAwsConf.failed.get shouldBe a[ConfigReaderException[_]]
    monixAwsConf.failed.get.getMessage should include("Key not found: 'credentials'")
  }

  it should "fail when credentials region is not present" in {
    import KebabConfigReader._

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

    Task(configSource.loadOrThrow[AppConf]).map(_.monixAws).attempt.asserting { monixAwsConf =>
      monixAwsConf.isLeft shouldBe true
      monixAwsConf.left.get shouldBe a[ConfigReaderException[_]]
      monixAwsConf.left.get.getMessage should include("Key not found: 'region'")
    }
  }

  it can "read config in kebabCase" in {
    MonixAwsConf.file(new File("aws-auth/src/test/resources/kebab-case.conf"), KebabCase).asserting { monixAwsConf =>
      monixAwsConf.credentials shouldBe a[DefaultCredentialsProvider]
      monixAwsConf.endpoint shouldBe Some(URI.create("kebab-case:12345"))
      monixAwsConf.region shouldBe Region.EU_WEST_1
    }
  }

  it can "read config in snake_case" in {
    MonixAwsConf.file(new File("aws-auth/src/test/resources/snake_case.conf"), SnakeCase).asserting { monixAwsConf =>
      monixAwsConf.credentials shouldBe a[DefaultCredentialsProvider]
      monixAwsConf.endpoint shouldBe Some(URI.create("snake:12345"))
      monixAwsConf.region shouldBe Region.EU_WEST_1
    }
  }

  it can "read config in PascalCase" in {
    MonixAwsConf.file(new File("aws-auth/src/test/resources/PascalCase.conf"), PascalCase).asserting { monixAwsConf =>
      monixAwsConf.credentials shouldBe a[DefaultCredentialsProvider]
      monixAwsConf.endpoint shouldBe Some(URI.create("PascalCase:12345"))
      monixAwsConf.region shouldBe Region.EU_WEST_1
    }
  }

  it can "read config in camelCase" in {
    MonixAwsConf.file(new File("aws-auth/src/test/resources/camelCase.conf"), CamelCase).asserting { monixAwsConf =>
      monixAwsConf.credentials shouldBe a[DefaultCredentialsProvider]
      monixAwsConf.endpoint shouldBe Some(URI.create("camelCase:12345"))
      monixAwsConf.region shouldBe Region.EU_WEST_1
    }
  }

  it should "read config in kebab-case by default from reference.conf " in {
    MonixAwsConf.load().asserting { monixAwsConf =>
      monixAwsConf.credentials shouldBe a[DefaultCredentialsProvider]
      monixAwsConf.endpoint shouldBe Some(URI.create("localhost:4566"))
      monixAwsConf.region shouldBe Region.EU_WEST_1
    }
  }

  it should "read config in camelCase by default when reading from path" in {
    MonixAwsConf.file(new File("aws-auth/src/test/resources/kebab-case.conf")).asserting { monixAwsConf =>
      monixAwsConf.credentials shouldBe a[DefaultCredentialsProvider]
      monixAwsConf.endpoint shouldBe Some(URI.create("kebab-case:12345"))
      monixAwsConf.region shouldBe Region.EU_WEST_1
    }

  }

}
