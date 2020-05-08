import sbt.Keys.sourceManaged
import sbt._

object Dependencies {

  object DependencyVersions {
    val AWS = "1.11.749"
    val DynamoDb = "2.10.60"
    val PureConfig = "0.12.3"
    val S3 = "2.10.50"
    val Monix = "3.1.0"
    val Circe = "0.11.1"
    val TypesafeConfig = "1.3.2"

    val Log4jScala = "11.0"
    val Log4j = "2.10.0"
    val ScalaLogging = "3.9.2"

    val Scalatest = "3.1.1"
    val Scalacheck = "1.14.0"
    val Mockito = "1.13.1"
    val Cats = "2.0.0"

    val AkkaStreams = "2.6.4"

    val Hadoop = "3.1.1"
  }

  private val CommonTestDependencies = Seq(
    "org.scalatest" %% "scalatest"   % DependencyVersions.Scalatest,
    "org.scalacheck" %% "scalacheck" % DependencyVersions.Scalacheck,
    "org.mockito" %% "mockito-scala" % DependencyVersions.Mockito
  )

  private val AkkaMain = Seq(
    "io.monix" %% "monix-reactive"          % DependencyVersions.Monix,
    "com.typesafe.akka" %% "akka-stream" % DependencyVersions.AkkaStreams
  )

  val Akka = AkkaMain ++ CommonTestDependencies.map(_ % Test)

  private val CommonMain = Seq(
    "io.monix" %% "monix-reactive"          % DependencyVersions.Monix
  )

  val Common = CommonMain ++ CommonTestDependencies.map(_ % Test)

  private val DynamoDbDependencies = Seq(
    "io.monix" %% "monix-reactive" % DependencyVersions.Monix,
    "com.amazonaws"                % "aws-java-sdk-core" % DependencyVersions.AWS,
    // "com.amazonaws"                       % "aws-java-sdk-dynamodb" % DependencyVersions.AWS, //todo compatibility with java sdk aws
    "software.amazon.awssdk"                % "dynamodb" % DependencyVersions.DynamoDb,
    "org.typelevel" %% "cats-core"          % DependencyVersions.Cats,
    "com.github.pureconfig" %% "pureconfig" % DependencyVersions.PureConfig
  )

  val DynamoDb = DynamoDbDependencies ++ CommonTestDependencies.map(_ % Test) ++ CommonTestDependencies.map(
    _                                                                 % IntegrationTest)

  private val HdfsDependecies = Seq(
    "io.monix" %% "monix-reactive"          % DependencyVersions.Monix,
    "org.apache.hadoop" % "hadoop-client" % DependencyVersions.Hadoop,
    "org.apache.hadoop" % "hadoop-common" % DependencyVersions.Hadoop % Test classifier "tests",
    "org.apache.hadoop" % "hadoop-hdfs" % DependencyVersions.Hadoop % Test classifier "tests",
    "org.apache.hadoop" % "hadoop-minicluster" % DependencyVersions.Hadoop
  )

  val Hdfs = HdfsDependecies ++ CommonTestDependencies.map(_ % Test)

  private val ParquetDependecies = Seq(
    "io.monix" %% "monix-reactive"          % DependencyVersions.Monix,
    "org.apache.parquet" % "parquet-avro" % "1.11.0",
    "org.apache.parquet" % "parquet-hadoop" % "1.11.0",
    "org.apache.parquet" % "parquet-protobuf" % "1.11.0",
    "com.twitter.elephantbird" % "elephant-bird" % "4.17",
    "org.apache.hadoop" % "hadoop-client" % "3.2.1",
    "org.apache.hadoop" % "hadoop-common" % "3.2.1",
    "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
  )

  val Parquet = ParquetDependecies ++ CommonTestDependencies.map(_ % Test)

  private val S3Dependecies = Seq(
    "io.monix" %% "monix-reactive"          % DependencyVersions.Monix,
    "software.amazon.awssdk"                % "s3" % DependencyVersions.S3,
    "org.typelevel" %% "cats-core"          % DependencyVersions.Cats,
    "com.github.pureconfig" %% "pureconfig" % DependencyVersions.PureConfig,
    "com.amazonaws"                         % "aws-java-sdk-core" % DependencyVersions.AWS % IntegrationTest,
    "com.amazonaws"                         % "aws-java-sdk-s3" % DependencyVersions.AWS %  IntegrationTest,
    "org.scalatestplus" %% "scalacheck-1-14" % "3.1.1.1" % Test
  )
  val S3 = S3Dependecies ++ CommonTestDependencies.map(_ % Test) ++ CommonTestDependencies.map(_ % IntegrationTest)

  private val RedisDependencies = Seq(
    "io.monix" %% "monix-reactive"          % DependencyVersions.Monix,
    "io.lettuce"                            % "lettuce-core" % "5.1.2.RELEASE",
    "org.typelevel" %% "cats-core"          % DependencyVersions.Cats,
    "com.github.pureconfig" %% "pureconfig" % DependencyVersions.PureConfig
  )

  val Redis = RedisDependencies ++ CommonTestDependencies.map(_ % Test)

}
