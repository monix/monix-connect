import sbt._

object Dependencies {

  object Versions {

    //main
    val Monix = "3.4.1"
    val AwsSdk = "2.17.276"
    val AkkaStreams = "2.6.18"
    val GCS = "1.107.0"
    val Hadoop = "3.3.4"
    val MongoScala = "4.1.1"
    val Lettuce = "6.1.9.RELEASE"
    val MongoReactiveStreams = "4.6.1"
    val Parquet = "1.12.3"
    val Elastic4s = "7.12.0"
    val Pureconfig = "0.17.1"
    val ScalaLogging = "3.9.5"
    val ScalaCompat = "2.8.1"

    //test
    val Scalatest = "3.2.12"
    val MonixTestingScalatest = "0.4.0"
    val Scalacheck = "1.16.0"
    val Mockito = "1.16.37"
    val GCNio = "0.123.19"
  }

   def commonDependencies(hasIt: Boolean = false): Seq[sbt.ModuleID] = {
    val common: Seq[ModuleID] = MonixDependency ++ CommonTestDependencies.map(_ % Test)
    if (hasIt) common ++ CommonTestDependencies.map(_                           % IntegrationTest)
    else common
  }

  private val MonixDependency = Seq("io.monix" %% "monix-reactive" % Versions.Monix)

  private val CommonTestDependencies = Seq(
    "org.scalacheck" %% "scalacheck"        % Versions.Scalacheck,
    "io.monix" %% "monix-testing-scalatest" % Versions.MonixTestingScalatest
  )

  val Akka = Seq("com.typesafe.akka" %% "akka-stream" % Versions.AkkaStreams) ++ commonDependencies(hasIt = false)

  val AwsAuth = Seq(
    "software.amazon.awssdk"                     % "auth" % Versions.AwsSdk,
    "com.github.pureconfig" %% "pureconfig-core" % Versions.Pureconfig) ++ commonDependencies(hasIt = false)

  val DynamoDb = Seq("software.amazon.awssdk" % "dynamodb" % Versions.AwsSdk) ++ commonDependencies(hasIt = true)

  val Hdfs = Seq(
    "org.apache.hadoop" % "hadoop-client"      % Versions.Hadoop,
    "org.apache.hadoop" % "hadoop-common"      % Versions.Hadoop,
    "org.apache.hadoop" % "hadoop-hdfs"        % Versions.Hadoop,
    "org.apache.hadoop" % "hadoop-minicluster" % Versions.Hadoop % Test
  ) ++ commonDependencies(hasIt = false)

  val MongoDb = Seq(
    "org.scala-lang.modules" %% "scala-collection-compat" % Versions.ScalaCompat,
    "org.mongodb"                                         % "mongodb-driver-reactivestreams" % Versions.MongoReactiveStreams,
    "org.mongodb.scala" %% "mongo-scala-bson"             % Versions.MongoScala % Test,
    "org.mongodb.scala" %% "mongo-scala-driver"           % Versions.MongoScala % Test,
    "org.mockito" %% "mockito-scala" % Versions.Mockito % Test cross CrossVersion.for3Use2_13
  ) ++ commonDependencies(hasIt = true)

  val Parquet = Seq(
    "org.apache.parquet" % "parquet-avro"   % Versions.Parquet,
    "org.apache.parquet" % "parquet-hadoop" % Versions.Parquet,
    "org.apache.hadoop"  % "hadoop-client"  % Versions.Hadoop,
    "org.apache.hadoop"  % "hadoop-common"  % Versions.Hadoop % Test,
    "org.mockito" %% "mockito-scala" % Versions.Mockito %Test cross CrossVersion.for3Use2_13
  ) ++ commonDependencies(hasIt = false)

  val S3 = Seq(
    "software.amazon.awssdk"                              % "s3" % Versions.AwsSdk,
    "org.scala-lang.modules" %% "scala-collection-compat" % Versions.ScalaCompat,
    "org.scalatestplus" %% "scalacheck-1-14"              % "3.1.4.0" % Test cross CrossVersion.for3Use2_13
  ) ++ commonDependencies(hasIt = true)

  val Redis = Seq(
    "io.lettuce"                                          % "lettuce-core" % Versions.Lettuce,
    "org.scala-lang.modules" %% "scala-collection-compat" % Versions.ScalaCompat,
    "com.thesamet.scalapb" %% "scalapb-runtime-grpc"      % scalapb.compiler.Version.scalapbVersion % IntegrationTest,
    "com.thesamet.scalapb" %% "scalapb-runtime"           % scalapb.compiler.Version.scalapbVersion % IntegrationTest,
  ) ++ commonDependencies(hasIt = true)

  val GCS = Seq(
    "org.scala-lang.modules" %% "scala-collection-compat" % Versions.ScalaCompat,
    "com.google.cloud"                                    % "google-cloud-storage" % Versions.GCS,
    "com.google.cloud"                                    % "google-cloud-nio" % Versions.GCNio % IntegrationTest,
    "commons-io"                                          % "commons-io" % "2.6" % Test
  ) ++ commonDependencies(hasIt = true)

  val Sqs = Seq(
    "org.scala-lang.modules" %% "scala-collection-compat" % Versions.ScalaCompat,
    "com.typesafe.scala-logging" %% "scala-logging"       % Versions.ScalaLogging,
    "software.amazon.awssdk"                              % "sqs" % Versions.AwsSdk
  ) ++ commonDependencies(hasIt = true)

  val Elasticsearch = Seq(
    "com.sksamuel.elastic4s" %% "elastic4s-client-esjava" % Versions.Elastic4s cross CrossVersion.for3Use2_13
  ) ++ commonDependencies(hasIt = true)

  val Benchmarks = Seq(
    "com.lightbend.akka" %% "akka-stream-alpakka-s3" % "2.0.2",
    "org.scalacheck" %% "scalacheck"                 % Versions.Scalacheck  cross CrossVersion.for3Use2_13,
    "dev.profunktor" %% "redis4cats-effects"         % "0.10.3",
    "io.chrisdavenport" %% "rediculous"              % "0.0.8",
    "io.laserdisc" %% "laserdisc-fs2"                % "0.4.1"
  ) ++ commonDependencies(hasIt = false)

}
