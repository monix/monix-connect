//libraryDependencies += "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"

lazy val root = (project in file("."))
  .settings(
    inThisBuild(List(
      organization := "scalona",
      scalaVersion := "2.13.1",
      version      := Version.version
    )),
    name := "monix-connect"
  )

lazy val akka = (project in file("akka"))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .settings(
    name := "monix-akka",
    libraryDependencies ++= Dependencies.Akka,
    version := "0.0.1"
  )
  .enablePlugins(JavaAppPackaging)

lazy val common = (project in file("common"))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .settings(
    name := "monix-common",
    libraryDependencies ++= Dependencies.Common,
    version := "0.0.1"
  )
  .enablePlugins(JavaAppPackaging)

lazy val dynamoDB = (project in file("dynamodb"))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .settings(
    name := "monix-dynamodb",
    libraryDependencies ++= Dependencies.DynamoDb,
    version := "0.0.1")
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .dependsOn(common)

lazy val hdfs = (project in file("hdfs"))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .settings(
    name := "monix-hdfs",
    libraryDependencies ++= Dependencies.Hdfs,
    version := "0.0.1"
  )

lazy val parquet = (project in file("parquet"))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .settings(
    name := "monix-parquet",
    libraryDependencies ++= Dependencies.Parquet,
    version := "0.0.1",
    PB.targets in Compile := Seq(
      scalapb.gen() -> (sourceManaged in Compile).value
    ),
    PB.targets in Compile := Seq(
      scalapb.gen(javaConversions=true) -> (sourceManaged in Compile).value,
      PB.gens.java -> (sourceManaged in Compile).value
    )
  )
  .enablePlugins(JavaAppPackaging, DockerPlugin)

lazy val s3 = (project in file("s3"))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .settings(
    name := "monix-s3",
    libraryDependencies ++= Dependencies.S3,
    version := "0.0.1"
  )
  .enablePlugins(JavaAppPackaging, DockerPlugin)

lazy val redis = (project in file("redis"))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .settings(
    name := "monix-redis",
    libraryDependencies ++= Dependencies.Redis,
    version := "0.0.1"
  )
  .enablePlugins(JavaAppPackaging, DockerPlugin)
