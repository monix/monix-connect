lazy val root = (project in file("."))
  .settings(
    inThisBuild(List(
      organization := "com.scalarc",
      scalaVersion := "2.13.1",
      version      := Version.version
    )),
    name := "monix-connectors"
  )

lazy val s3 = (project in file("s3"))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .settings(
    name := "monix-s3",
    libraryDependencies ++= Dependencies.S3,
    version := "0.0.1"
  )
  .enablePlugins(JavaAppPackaging, DockerPlugin)

lazy val dynamoDB = (project in file("dynamodb"))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .settings(
    name := "monix-dynamodb",
    libraryDependencies ++= Dependencies.DynamoDb,
    version := "0.0.1"
  )
  .enablePlugins(JavaAppPackaging, DockerPlugin)



