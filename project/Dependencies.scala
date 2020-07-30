import sbt._

object Dependencies {

  object DependencyVersions {
    val AWS = "1.11.831"
    val DynamoDb = "2.10.60"
    val PureConfig = "0.12.3"
    val S3 = "2.10.91"
    val Monix = "3.2.0"
    val AkkaStreams = "2.6.4"
    val Hadoop = "3.1.3"
    val GCS = "1.107.0"
    val Cats_Effect = "2.1.3"
    //test
    val Scalatest = "3.1.2"
    val Scalacheck = "1.14.0"
    val Mockito = "1.14.8"
    val Cats = "2.0.0"
    val GCNio = "0.121.2"
  }

  private def commonDependencies(hasIntegrationTest: Boolean = false): Seq[sbt.ModuleID] = {
    val common: Seq[ModuleID] = CommonProjectDependencies ++ CommonTestDependencies.map(_ % Test)
    if (hasIntegrationTest) common ++ CommonTestDependencies.map(_ % IntegrationTest)
    else common
  }

  private val CommonProjectDependencies = Seq(
    "io.monix" %% "monix-reactive" % DependencyVersions.Monix,
    "org.scala-lang.modules" %% "scala-collection-compat" % "2.1.6", //todo use as replacement for `collection.JavaConverters`
    "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.0"
   // "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
    //"org.slf4j" % "log4j-over-slf4j" % "1.7.30"
  )

  private val CommonTestDependencies = Seq(
    "org.scalatest" %% "scalatest" % DependencyVersions.Scalatest,
    "org.scalacheck" %% "scalacheck" % DependencyVersions.Scalacheck,
    "org.mockito" %% "mockito-scala" % DependencyVersions.Mockito
  )

  private val AkkaMain = Seq(
    "com.typesafe.akka" %% "akka-stream" % DependencyVersions.AkkaStreams
  )

  val Akka = AkkaMain ++ CommonProjectDependencies ++ CommonTestDependencies.map(_ % Test)

  private val DynamoDbDependencies = Seq(
    "com.amazonaws" % "aws-java-sdk-core" % DependencyVersions.AWS,
    // "com.amazonaws"                       % "aws-java-sdk-dynamodb" % DependencyVersions.AWS, //todo compatibility with java sdk aws
    "software.amazon.awssdk" % "dynamodb" % DependencyVersions.DynamoDb)

  val DynamoDb = DynamoDbDependencies ++ CommonProjectDependencies ++ CommonTestDependencies.map(_ % Test) ++ CommonTestDependencies.map(
    _ % IntegrationTest)

  private val HdfsDependecies = Seq(
    "org.apache.hadoop" % "hadoop-client" % DependencyVersions.Hadoop,
    "org.apache.hadoop" % "hadoop-common" % DependencyVersions.Hadoop % Test classifier "tests",
    "org.apache.hadoop" % "hadoop-hdfs" % DependencyVersions.Hadoop % Test classifier "tests",
    "org.apache.hadoop" % "hadoop-minicluster" % DependencyVersions.Hadoop
  )

  val Hdfs = HdfsDependecies ++ commonDependencies(hasIntegrationTest = false)

  private val ParquetDependecies = Seq(
    "io.monix" %% "monix-reactive" % DependencyVersions.Monix,
    "org.apache.parquet" % "parquet-avro" % "1.11.0",
    "org.apache.parquet" % "parquet-hadoop" % "1.11.0",
    "org.apache.parquet" % "parquet-protobuf" % "1.11.0",
    "com.twitter.elephantbird" % "elephant-bird" % "4.17",
    "org.apache.hadoop" % "hadoop-client" % "3.2.1",
    "org.apache.hadoop" % "hadoop-common" % "3.2.1",
    "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
  )

  val Parquet = ParquetDependecies ++ CommonProjectDependencies ++ CommonTestDependencies.map(_ % Test)

  private val S3Dependecies = Seq(
    "software.amazon.awssdk" % "s3" % DependencyVersions.S3,
    "com.amazonaws" % "aws-java-sdk-core" % DependencyVersions.AWS % IntegrationTest,
    "com.amazonaws" % "aws-java-sdk-s3" % DependencyVersions.AWS % IntegrationTest,
    "org.scalatestplus" %% "scalacheck-1-14" % "3.1.1.1" % Test
  )
  val S3 = S3Dependecies ++ CommonProjectDependencies ++ CommonTestDependencies.map(_ % Test) ++ CommonTestDependencies.map(_ % IntegrationTest)

  private val RedisDependencies = Seq(
    "io.lettuce" % "lettuce-core" % "5.1.8.RELEASE",
    "com.github.pureconfig" %% "pureconfig" % DependencyVersions.PureConfig
  )

  val Redis = RedisDependencies ++ CommonProjectDependencies ++ CommonTestDependencies.map(_ % Test)

  private val GcsDependencies = Seq(
    "org.typelevel"     %% "cats-core"            % DependencyVersions.Cats,
    "com.google.cloud"   % "google-cloud-storage" % DependencyVersions.GCS,
    "org.typelevel" %% "cats-effect" % DependencyVersions.Cats_Effect,
    "com.google.cloud" % "google-cloud-nio" % DependencyVersions.GCNio % IntegrationTest,
    "commons-io" % "commons-io" % "2.6" % Test
  )

  val GCS = GcsDependencies  ++ CommonProjectDependencies ++ CommonTestDependencies
}
