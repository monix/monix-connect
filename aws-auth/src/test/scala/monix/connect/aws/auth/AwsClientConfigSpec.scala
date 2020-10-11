package monix.connect.aws.auth

import org.scalatest.flatspec.AnyFlatSpec
import pureconfig.ConfigSource
import pureconfig._
import pureconfig.generic.auto._
import AwsClientConf._
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.regions.Region

class AwsClientConfigSpec extends AnyFlatSpec with Matchers {

  s"${AwsClientConf}" should "load from default config file" in {
    val awsClientConf = AwsClientConf.load.toOption.get

    awsClientConf.credentials.provider shouldBe Provider.Default
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
    awsClientConf.credentials.provider shouldBe Provider.Default
    awsClientConf.endpoint.isDefined shouldBe false
    awsClientConf.httpClient.isDefined shouldBe false
    awsClientConf.region shouldBe Region.AWS_GLOBAL
  }


}
