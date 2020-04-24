
val IT = config("it") extend Test
lazy val root = (project in file("."))
  .configs(IntegrationTest, IT)
  .settings(
    Defaults.itSettings,
    inThisBuild(List(
      organization := "monix",
      scalaVersion := "2.13.1",
      version      := Version.version
    )),
    name := "monix-connect",
    scalafmtOnCompile := true
)
  .aggregate(akka, dynamoDB, hdfs, parquet, s3, redis)
  .dependsOn(akka, dynamoDB, hdfs, parquet, s3, redis)


lazy val akka = (project in file("akka"))
  .settings(
    name := "monix-akka",
    libraryDependencies ++= Dependencies.Akka,
    version := Version.version,
    scalafmtOnCompile := true
  )
  .enablePlugins(JavaAppPackaging)

lazy val common = (project in file("common"))
  .settings(
    name := "monix-common",
    libraryDependencies ++= Dependencies.Common,
    version := Version.version,
    scalafmtOnCompile := true
  )

lazy val dynamoDB = (project in file("dynamodb"))
  .configs(IntegrationTest, IT)
  .settings(
    Defaults.itSettings,
    name := "monix-dynamodb",
    libraryDependencies ++= Dependencies.DynamoDb,
    version := Version.version,
    scalafmtOnCompile := true
  )
  .dependsOn(common)

lazy val hdfs = (project in file("hdfs"))
  .settings(
    name := "monix-hdfs",
    libraryDependencies ++= Dependencies.Hdfs,
    version := Version.version,
    scalafmtOnCompile := true
  )

lazy val parquet = (project in file("parquet"))
  .settings(
    name := "monix-parquet",
    libraryDependencies ++= Dependencies.Parquet,
    version := Version.version,
    scalafmtOnCompile := true,
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
  .configs(IntegrationTest, IT)
  .settings(
    Defaults.itSettings,
    scalafmtOnCompile := true,
    name := "monix-s3",
    libraryDependencies ++= Dependencies.S3,
    version := Version.version
  )

lazy val redis = (project in file("redis"))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .settings(
    scalafmtOnCompile := true,
    name := "monix-redis",
    libraryDependencies ++= Dependencies.Redis,
    version := Version.version
  )
  .enablePlugins(JavaAppPackaging, DockerPlugin)
