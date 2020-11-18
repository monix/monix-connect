import sbt._

object Dependencies {

  object Versions {

    //main
    val AwsSdk = "2.15.19"
    val AkkaStreams = "2.6.9"
    val AWS = "1.11.904"
    val Cats_Effect = "2.1.3"
    val DynamoDb = "2.10.60"
    val GCS = "1.107.0"
    val Hadoop = "3.1.4"
    val Monix = "3.2.0"
    val MongoScala = "4.1.1"
    val MongoReactiveStreams = "4.1.1"
    val S3 = "2.14.21"
    val Parquet = "1.11.1"
    val Pureconfig = "0.14.0"

    //test
    val Scalatest = "3.2.2"
    val Scalacheck = "1.14.0"
    val Mockito = "1.15.0"
    val GCNio = "0.121.2"
  }

  private def testDependencies(hasIt: Boolean = false): Seq[sbt.ModuleID] = {
    val common: Seq[ModuleID] = MonixDependency ++ CommonTestDependencies.map(_ % Test)
    if (hasIt) common ++ CommonTestDependencies.map(_                           % IntegrationTest)
    else common
  }

  private val MonixDependency = Seq(
    "io.monix" %% "monix-reactive"                        % Versions.Monix,
    "org.scala-lang.modules" %% "scala-collection-compat" % "2.2.0", //todo used as replacement for `collection.JavaConverters`
    "org.scala-lang.modules" %% "scala-java8-compat"      % "0.9.0"
  )

  private val CommonTestDependencies = Seq(
    "org.scalatest" %% "scalatest"   % Versions.Scalatest,
    "org.scalacheck" %% "scalacheck" % Versions.Scalacheck,
    "org.mockito" %% "mockito-scala" % Versions.Mockito
  )

  val Akka = Seq("com.typesafe.akka" %% "akka-stream" % Versions.AkkaStreams) ++ testDependencies(hasIt = false)

  val AwsAuth = Seq(
    "io.monix" %% "monix-reactive" % Versions.Monix,
    "software.amazon.awssdk" % "auth" % Versions.AwsSdk,
    "com.github.pureconfig" %% "pureconfig" % Versions.Pureconfig) ++ testDependencies(hasIt = false)

  val Benchmarks = Seq(
   "com.lightbend.akka" %% "akka-stream-alpakka-s3" % "2.0.2",
    "org.scalacheck" %% "scalacheck" % Versions.Scalacheck,
    "dev.profunktor"    %% "redis4cats-effects" % "0.10.3",
    "io.chrisdavenport" %% "rediculous"         % "0.0.8",
    "io.laserdisc"      %% "laserdisc-fs2"      % "0.4.1"
  )++ testDependencies(hasIt = false)

  val DynamoDb = Seq(
    "com.amazonaws"          % "aws-java-sdk-core" % Versions.AWS,
    "software.amazon.awssdk" % "dynamodb"          % Versions.DynamoDb
  ) ++ testDependencies(hasIt = true)

  val Hdfs = Seq(
    "org.apache.hadoop" % "hadoop-client"      % Versions.Hadoop,
    "org.apache.hadoop" % "hadoop-common"      % Versions.Hadoop,
    "org.apache.hadoop" % "hadoop-hdfs"        % Versions.Hadoop,
    "org.apache.hadoop" % "hadoop-minicluster" % Versions.Hadoop % Test
  ) ++ testDependencies(hasIt = false)

  val MongoDb = Seq(
    "org.mongodb"                               % "mongodb-driver-reactivestreams" % Versions.MongoReactiveStreams,
    "org.mongodb.scala" %% "mongo-scala-bson"   % Versions.MongoScala,
    "org.mongodb.scala" %% "mongo-scala-driver" % Versions.MongoScala
  ) ++ testDependencies(hasIt = true)

  val Parquet = Seq(
    "org.apache.parquet" % "parquet-avro"     % Versions.Parquet,
    "org.apache.parquet" % "parquet-hadoop"   % Versions.Parquet,
    "org.apache.parquet" % "parquet-protobuf" % Versions.Parquet,
    "org.apache.hadoop" % "hadoop-client" % Versions.Hadoop,
    "org.apache.hadoop" % "hadoop-common" % Versions.Hadoop % Test,
    "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
  ) ++ testDependencies(hasIt = false)

  val S3 = Seq(
    "software.amazon.awssdk" % "s3" % Versions.AwsSdk,
    "org.scalatestplus" %% "scalacheck-1-14" % "3.1.1.1" % Test
  ) ++ testDependencies(hasIt = true)

  val Redis = Seq(
    "io.lettuce" % "lettuce-core" % "5.1.8.RELEASE"
  ) ++ testDependencies(hasIt = true)

  val GCS = Seq(
    "com.google.cloud"               % "google-cloud-storage" % Versions.GCS,
    "org.typelevel" %% "cats-effect" % Versions.Cats_Effect,
    "com.google.cloud"               % "google-cloud-nio" % Versions.GCNio % IntegrationTest,
    "commons-io"                     % "commons-io" % "2.6" % Test
  ) ++ testDependencies(hasIt = true)
}
