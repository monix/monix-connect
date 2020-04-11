import sbt._

object Dependencies {

  object DependencyVersions {
    val AWS = "1.11.749"
    val AwsSdk2Version = "2.10.60"
    val PureConfig = "0.12.3"
    val Monix = "3.1.0"
    val Circe = "0.11.1"
    val TypesafeConfig = "1.3.2"

    val Log4jScala = "11.0"
    val Log4j = "2.10.0"
    val ScalaLogging = "3.9.2"

    val Scalatest = "3.1.1"
    val Scalacheck = "1.14.3"
    val Mockito = "1.13.1"
    val Cats = "2.0.0"
  }




  private val CommonTestDependencies = Seq(
    "org.scalatest" %% "scalatest"   % DependencyVersions.Scalatest,
    "org.scalacheck" %% "scalacheck" % DependencyVersions.Scalacheck,
    "org.mockito" %% "mockito-scala" % DependencyVersions.Mockito)

  private val CommonMain = Seq(
    "io.monix" %% "monix-reactive"          % DependencyVersions.Monix
  )

  val Common = CommonMain ++ CommonTestDependencies.map(_ % Test)

  private val S3Dependecies = Seq(
    "io.monix" %% "monix-reactive"          % DependencyVersions.Monix,
    "com.amazonaws"                         % "aws-java-sdk-core" % DependencyVersions.AWS,
    "com.amazonaws"                         % "aws-java-sdk-s3" % DependencyVersions.AWS,
    "org.typelevel" %% "cats-core"          % DependencyVersions.Cats,
    "com.github.pureconfig" %% "pureconfig" % DependencyVersions.PureConfig
  )

  val S3 = S3Dependecies ++ CommonTestDependencies.map(_ % Test) ++ CommonTestDependencies.map(_ % IntegrationTest)

  private val DynamoDbDependencies = Seq(
    "io.monix" %% "monix-reactive" % DependencyVersions.Monix,
     "com.amazonaws"                         % "aws-java-sdk-core" % DependencyVersions.AWS,
    // "com.amazonaws"                       % "aws-java-sdk-dynamodb" % DependencyVersions.AWS, //todo compatibility with java sdk aws
    "software.amazon.awssdk"                % "dynamodb" % DependencyVersions.AwsSdk2Version,
    "org.typelevel" %% "cats-core"          % DependencyVersions.Cats,
    "com.github.pureconfig" %% "pureconfig" % DependencyVersions.PureConfig
  )

  val DynamoDb = DynamoDbDependencies ++ CommonTestDependencies.map(_ % Test) ++ CommonTestDependencies.map(_ % IntegrationTest)

  private val RedisDependencies = Seq(
    "io.monix" %% "monix-reactive" % DependencyVersions.Monix,
    "io.lettuce" % "lettuce-core" % "5.1.2.RELEASE",
    "org.typelevel" %% "cats-core"          % DependencyVersions.Cats,
    "com.github.pureconfig" %% "pureconfig" % DependencyVersions.PureConfig
  )

  val Redis = RedisDependencies ++ CommonTestDependencies.map(_ % Test) ++ CommonTestDependencies.map(_ % IntegrationTest)

}
