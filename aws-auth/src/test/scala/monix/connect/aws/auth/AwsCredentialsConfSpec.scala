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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderException
import monix.connect.aws.auth.configreader.KebabConfigReader
import software.amazon.awssdk.auth.credentials.{
  AnonymousCredentialsProvider,
  AwsSessionCredentials,
  DefaultCredentialsProvider,
  EnvironmentVariableCredentialsProvider,
  InstanceProfileCredentialsProvider,
  ProfileCredentialsProvider,
  StaticCredentialsProvider,
  SystemPropertyCredentialsProvider
}

import scala.util.Try

class AwsCredentialsConfSpec extends AnyFlatSpec with Matchers {
  import KebabConfigReader._
  s"$AwsCredentialsConf" should "allow to set aws default credentials" in {
    //given
    val configSource = ConfigSource.string(
      "" +
        """
          |{
          |  provider: "default"
          |}
          |""".stripMargin)
    //when
    val credentialsConf = configSource.loadOrThrow[AwsCredentialsConf]

    //then
    credentialsConf.credentialsProvider shouldBe a[DefaultCredentialsProvider]
  }

  it should "set aws anonymous credentials" in {
    //given
    val configSource = ConfigSource.string(
      "" +
        """
          |{
          |  provider: "anonymous"
          |}
          |""".stripMargin)

    //when
    val credentialsConf = configSource.loadOrThrow[AwsCredentialsConf]

    //then
    credentialsConf.credentialsProvider shouldBe a[AnonymousCredentialsProvider]

  }

  it should "set aws environment credentials" in {
    //given
    val configSource = ConfigSource.string(
      "" +
        """
          |{
          |  provider: "environment"
          |}
          |""".stripMargin)

    //when
    val credentialsConf = configSource.loadOrThrow[AwsCredentialsConf]

    //then
    credentialsConf.credentialsProvider shouldBe a[EnvironmentVariableCredentialsProvider]
  }

  it should "set aws instance credentials" in {
    //given
    val configSource = ConfigSource.string(
      "" +
        """
          |{
          |  provider: "instance"
          |}
          |""".stripMargin)

    //when
    val credentialsConf = configSource.loadOrThrow[AwsCredentialsConf]

    //then
    credentialsConf.credentialsProvider shouldBe a[InstanceProfileCredentialsProvider]
  }

  it should "set aws profile `default` credentials" in {
    //given
    val configSource = ConfigSource.string(
      "" +
        """
          |{
          |  provider: "profile"
          |}
          |""".stripMargin)

    //when
    val credentialsConf = configSource.loadOrThrow[AwsCredentialsConf]

    //then
    credentialsConf.credentialsProvider shouldBe a[ProfileCredentialsProvider]
    credentialsConf.profileName.isDefined shouldBe false
  }

  it should "set aws profile credentials" in {
    //given
    val profileName = "dev"
    val configSource = ConfigSource.string(
      "" +
        s"""
           |{
           |  provider: "profile"
           |  profile-name: "$profileName"
           |}
           |""".stripMargin)

    //when
    val credentialsConf = configSource.loadOrThrow[AwsCredentialsConf]

    //then
    credentialsConf.credentialsProvider shouldBe a[ProfileCredentialsProvider]
    credentialsConf.profileName.isDefined shouldBe true
    credentialsConf.profileName.get shouldBe profileName
  }

  it should "set aws static credentials" in {
    //given
    val accessKeyId = "sample-key"
    val secretAccessKey = "sample-secret"
    val configSource = ConfigSource.string(
      "" +
        s"""
           |{
           |  provider: "static"
           |  static {
           |    access-key-id: "$accessKeyId"
           |    secret-access-key: "$secretAccessKey"
           |  }
           |}
           |""".stripMargin)

    //when
    val credentialsConf = configSource.loadOrThrow[AwsCredentialsConf]

    //then
    credentialsConf.credentialsProvider shouldBe a[StaticCredentialsProvider]
    val awsCredentials = credentialsConf.credentialsProvider.resolveCredentials()
    awsCredentials.accessKeyId() shouldBe accessKeyId
    awsCredentials.secretAccessKey() shouldBe secretAccessKey
  }

  it should "set aws static session credentials" in {
    //given
    val accessKeyId = "sample-key"
    val secretAccessKey = "sample-secret"
    val sessionToken = "sample-session-token"
    val configSource = ConfigSource.string(
      "" +
        s"""
           |{
           |  provider: "static"
           |  static {
           |    access-key-id: "$accessKeyId"
           |    secret-access-key: "$secretAccessKey"
           |    session-token: "$sessionToken"
           |  }
           |}
           |""".stripMargin)

    //when
    val credentialsConf = configSource.loadOrThrow[AwsCredentialsConf]

    //then
    credentialsConf.credentialsProvider shouldBe a[StaticCredentialsProvider]
    val staticCreeds = credentialsConf.credentialsProvider.asInstanceOf[StaticCredentialsProvider]
    val sessionCreeds = staticCreeds.resolveCredentials().asInstanceOf[AwsSessionCredentials]
    sessionCreeds.sessionToken() shouldBe sessionToken
    sessionCreeds.accessKeyId() shouldBe accessKeyId
    sessionCreeds.secretAccessKey() shouldBe secretAccessKey
  }

  it should "fallback to default when provider was set to static but there was no static credentials" in {
    //given
    val configSource = ConfigSource.string(
      "" +
        s"""
           |{
           |  provider: "static"
           |}
           |""".stripMargin)

    //when
    val credentialsConf = configSource.loadOrThrow[AwsCredentialsConf]

    //then
    credentialsConf.credentialsProvider shouldBe a[DefaultCredentialsProvider]
  }

  it should "set aws system credentials" in {
    //given
    val configSource = ConfigSource.string(
      "" +
        s"""
           |{
           |  provider: "system"
           |}
           |""".stripMargin)

    //when
    val credentialsConf = configSource.loadOrThrow[AwsCredentialsConf]

    //then
    credentialsConf.credentialsProvider shouldBe a[SystemPropertyCredentialsProvider]
  }

  it should "fail when provider is not set" in {
    //given
    val configSource = ConfigSource.string(
      "" +
        s"""
           |{
           |  static {
           |    access-key-id: ""
           |    secret-access-key: ""
           |    session-token: ""
           |  }
           |}
           |""".stripMargin)

    //when
    val credentialsConf = Try(configSource.loadOrThrow[AwsCredentialsConf])

    //then
    credentialsConf.isFailure shouldBe true
    credentialsConf.failed.get shouldBe a[ConfigReaderException[_]]
    credentialsConf.failed.get.getMessage should include("Key not found: 'provider'.")
  }
}
