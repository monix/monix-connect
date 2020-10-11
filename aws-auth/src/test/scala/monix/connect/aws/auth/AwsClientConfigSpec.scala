package monix.connect.aws.auth

import org.scalatest.flatspec.AnyFlatSpec
import pureconfig.ConfigSource
import pureconfig._
import pureconfig.generic.auto._
import AwsClientConf._
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.auth.credentials.{
  AnonymousCredentialsProvider,
  DefaultCredentialsProvider,
  StaticCredentialsProvider
}

class AwsClientConfigSpec extends AnyFlatSpec with Matchers {

  s"${AwsClientConf}" should "load from default config file" in {
    println("Config: " + AwsClientConf.load)
  }

  it should "allow to use aws anonymous credentials" in {
    val configSource = ConfigSource.string(
      "" +
        """
          |{
          |  provider: "default"
          |}
          |""".stripMargin)
    val credentialsConf = configSource.loadOrThrow[CredentialsConf]

    credentialsConf.credentialsProvider shouldBe a[DefaultCredentialsProvider]
  }

  it should "allow to configure aws anonymous credentials" in {
    val configSource = ConfigSource.string(
      "" +
        """
          |{
          |  provider: "anonymous"
          |}
          |""".stripMargin)
    val credentialsConf = configSource.loadOrThrow[CredentialsConf]

    credentialsConf.credentialsProvider shouldBe a[AnonymousCredentialsProvider]

  }

  it should "allow to configure aws static credentials" in {
    val accessKeyId = "sample-key"
    val secretAccessKey = "sample-secret"
    val configSource = ConfigSource.string(
      "" +
        s"""
          |{
          |  provider: "static"
          |  static-credentials {
          |    access-key-id: "$accessKeyId"
          |    secret-access-key: "$secretAccessKey"
          |  }
          |}
          |""".stripMargin)
    val credentialsConf = configSource.loadOrThrow[CredentialsConf]

    credentialsConf.credentialsProvider shouldBe a[StaticCredentialsProvider]
    val awsCredentials = credentialsConf.credentialsProvider.resolveCredentials()
    awsCredentials.accessKeyId() shouldBe accessKeyId
    awsCredentials.secretAccessKey() shouldBe secretAccessKey
  }

  it should "allow to configure aws static session credentials" in {
    val accessKeyId = "sample-key"
    val secretAccessKey = "sample-secret"
    val configSource = ConfigSource.string(
      "" +
        s"""
           |{
           |  provider: "static"
           |  static-credentials {
           |    access-key-id: "$accessKeyId"
           |    secret-access-key: "$secretAccessKey"
           |  }
           |}
           |""".stripMargin)
    val credentialsConf = configSource.loadOrThrow[CredentialsConf]

    credentialsConf.credentialsProvider shouldBe a[StaticCredentialsProvider]
    val awsCredentials = credentialsConf.credentialsProvider.resolveCredentials()
    awsCredentials.accessKeyId() shouldBe accessKeyId
    awsCredentials.secretAccessKey() shouldBe secretAccessKey
  }
}
