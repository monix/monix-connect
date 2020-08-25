import sbt._

object Dependencies {

  object Versions {
    
    //main
    val AkkaStreams = "2.6.4"
    val AWS = "1.11.848"
    val Cats_Effect = "2.1.3"
    val DynamoDb = "2.10.60"
    val GCS = "1.107.0"
    val Hadoop = "3.1.4"
    val Monix = "3.2.0"
    val MongoScala = "2.9.0"
    val MongoReactiveStreams = "4.1.0"
    val S3 = "2.14.3"
    val Parquet = "1.11.1"

    //test
    val Scalatest = "3.2.2"
    val Scalacheck = "1.14.0"
    val Mockito = "1.14.8"
    val GCNio = "0.121.2"
  }

  private def commonDependencies(hasIntegrationTest: Boolean = false): Seq[sbt.ModuleID] = {
    val common: Seq[ModuleID] = CommonProjectDependencies ++ CommonTestDependencies.map(_ % Test)
    if (hasIntegrationTest) common ++ CommonTestDependencies.map(_ % IntegrationTest)
    else common
  }

  private val CommonProjectDependencies = Seq(
    "io.monix" %% "monix-reactive" % Versions.Monix,
    "org.scala-lang.modules" %% "scala-collection-compat" % "2.1.6", //todo used as replacement for `collection.JavaConverters`
    "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.0"
  )

  private val CommonTestDependencies = Seq(
    "org.scalatest" %% "scalatest" % Versions.Scalatest,
    "org.scalacheck" %% "scalacheck" % Versions.Scalacheck,
    "org.mockito" %% "mockito-scala" % Versions.Mockito
  )

  val Akka = Seq("com.typesafe.akka" %% "akka-stream" % Versions.AkkaStreams
  ) ++ commonDependencies(hasIntegrationTest = false)

  val DynamoDb = Seq(
    "com.amazonaws" % "aws-java-sdk-core" % Versions.AWS,
    // "com.amazonaws"  % "aws-java-sdk-dynamodb" % DependencyVersions.AWS, //todo compatibility with java sdk aws
    "software.amazon.awssdk" % "dynamodb" % Versions.DynamoDb
  ) ++ commonDependencies(hasIntegrationTest = true)

  val Hdfs = Seq(
    "org.apache.hadoop" % "hadoop-client" % Versions.Hadoop,
    "org.apache.hadoop" % "hadoop-common" % Versions.Hadoop,
    "org.apache.hadoop" % "hadoop-hdfs" % Versions.Hadoop,
    "org.apache.hadoop" % "hadoop-minicluster" % Versions.Hadoop % Test
  ) ++ commonDependencies(hasIntegrationTest = false)

  val MongoDb = Seq(
    "org.mongodb" % "mongodb-driver-reactivestreams" % Versions.MongoReactiveStreams,
    "org.mongodb.scala" %% "mongo-scala-bson" % Versions.MongoScala,
    "org.mongodb.scala" %% "mongo-scala-driver" % Versions.MongoScala
  ) ++ commonDependencies(hasIntegrationTest = true)

  val Parquet = Seq(
    "org.apache.parquet" % "parquet-avro" % Versions.Parquet,
    "org.apache.parquet" % "parquet-hadoop" % Versions.Parquet,
    "org.apache.parquet" % "parquet-protobuf" % Versions.Parquet,
    //"com.twitter.elephantbird" % "elephant-bird" % "4.17",
    "org.apache.hadoop" % "hadoop-client" % Versions.Hadoop % Test,
    "org.apache.hadoop" % "hadoop-common" % Versions.Hadoop % Test,
    "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
  ) ++ commonDependencies(hasIntegrationTest = false)

  val S3 = Seq(
    "software.amazon.awssdk" % "s3" % Versions.S3,
    "com.amazonaws" % "aws-java-sdk-core" % Versions.AWS % IntegrationTest,
    "com.amazonaws" % "aws-java-sdk-s3" % Versions.AWS % IntegrationTest,
    "org.scalatestplus" %% "scalacheck-1-14" % "3.1.1.1" % Test
  ) ++ commonDependencies(hasIntegrationTest = true)

  val Redis = Seq(
    "io.lettuce" % "lettuce-core" % "5.1.8.RELEASE"
  ) ++ commonDependencies(hasIntegrationTest = true)

  val GCS = Seq(
    "com.google.cloud"   % "google-cloud-storage" % Versions.GCS,
    "org.typelevel" %% "cats-effect" % Versions.Cats_Effect,
    "com.google.cloud" % "google-cloud-nio" % Versions.GCNio % IntegrationTest,
    "commons-io" % "commons-io" % "2.6" % Test
  ) ++ commonDependencies(hasIntegrationTest = true)
}
