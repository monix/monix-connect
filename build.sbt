import Dependencies.Versions
import sbt.Keys.version

val monixConnectSeries = "0.7.0"

inThisBuild(List(
  organization := "io.monix",
  homepage := Some(url("https://connect.monix.io")),
  licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
  developers := List(
    Developer(
      "paualarco",
      "Pau Alarcón Cerdan",
      "pau.alarcon.b@gmail.com",
      url("https://connect.monix.io")
    )
  )
))

skip in publish := true //required by sbt-ci-release

def sharedSettings(publishForScala3: Boolean= true) = {
  Seq(
    scalaVersion := "2.13.11",
    crossScalaVersions := Seq("2.12.17", "2.13.11") ++ (if (publishForScala3) Seq("3.1.2") else Seq.empty)
  ,
    publishArtifact := (CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((3, _)) => publishForScala3
      case _ => true
    }),
    scalafmtOnCompile := false
  ,
    mimaFailOnNoPrevious := false,
    scalacOptions ++= Seq(
    // warnings
    "-unchecked", // able additional warnings where generated code depends on assumptions
    "-deprecation", // emit warning for usages of deprecated APIs
    "-feature", // emit warning usages of features that should be imported explicitly
    // Features enabled by default
    "-language:higherKinds",
    "-language:implicitConversions",
    "-language:experimental.macros"
  ),
  //warnUnusedImports
  scalacOptions in(Compile, console) ++= Seq("-Ywarn-unused:imports")
  ,
  // Linter
  scalacOptions ++= Seq(
    "-Ywarn-unused:imports", // Warn if an import selector is not referenced.
    "-Ywarn-dead-code", // Warn when dead code is identified.
    // Turns all warnings into errors ;-)
    //temporary disabled for mongodb warn, -YWarn (2.13) and Silencer (2.12) should fix it...
    //"-Xfatal-warnings", //Turning of fatal warnings for the moment
    // Enables linter options
    "-Xlint:adapted-args", // warn if an argument list is modified to match the receiver
    "-Xlint:infer-any", // warn when a type argument is inferred to be `Any`
    "-Xlint:missing-interpolator", // a string literal appears to be missing an interpolator id
    "-Xlint:doc-detached", // a ScalaDoc comment appears to be detached from its element
    "-Xlint:private-shadow", // a private field (or class parameter) shadows a superclass field
    "-Xlint:type-parameter-shadow", // a local type parameter shadows a type already in scope
    "-Xlint:poly-implicit-overload", // parameterized overloaded implicit methods are not visible as view bounds
    "-Xlint:option-implicit", // Option.apply used implicit view
    "-Xlint:delayedinit-select", // Selecting member of DelayedInit
    //"-Xlint:package-object-classes" // Class or object defined in package object
  )
  ,

  // ScalaDoc settings
  scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings")
  ,
  autoAPIMappings := true
  ,
  scalacOptions in ThisBuild ++= Seq(
    // Note, this is used by the doc-source-url feature to determine the
    // relative path of a given source file. If it's not a prefix of a the
    // absolute path of the source file, the absolute path of that file
    // will be put into the FILE_SOURCE variable, which is
    // definitely not what we want.
    "-sourcepath",
    file(".").getAbsolutePath.replaceAll("[.]$", "")
  )
  ,
  parallelExecution in Test := true
  ,
  parallelExecution in ThisBuild := true
  ,
  testForkedParallel in Test := true
  ,
  testForkedParallel in ThisBuild := true
  ,
  concurrentRestrictions in Global += Tags.limit(Tags.Test, 3)
  ,
  logBuffered in Test := false
  ,
  logBuffered in IntegrationTest := false
  ,
  //dependencyClasspath in IntegrationTest := (dependencyClasspath in IntegrationTest).value ++ (exportedProducts in Test).value,
  // https://github.com/sbt/sbt/issues/2654
  incOptions := incOptions.value.withLogRecompileOnMacro(false)
  ,
  pomIncludeRepository := { _ => false }
  , // removes optional dependencies

  // ScalaDoc settings
  autoAPIMappings := true
  ,
  apiURL := Some(url("https://monix.github.io/monix-connect/api/"))
  ,

  headerLicense := Some(HeaderLicense.Custom(
    """|Copyright (c) 2020-2021 by The Monix Connect Project Developers.
       |See the project homepage at: https://connect.monix.io
       |
       |Licensed under the Apache License, Version 2.0 (the "License");
       |you may not use this file except in compliance with the License.
       |You may obtain a copy of the License at
       |
       |    http://www.apache.org/licenses/LICENSE-2.0
       |
       |Unless required by applicable law or agreed to in writing, software
       |distributed under the License is distributed on an "AS IS" BASIS,
       |WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
       |See the License for the specific language governing permissions and
       |limitations under the License."""
      .stripMargin))
  ,

  doctestTestFramework := DoctestTestFramework.ScalaTest
  ,
  doctestTestFramework := DoctestTestFramework.ScalaCheck
  ,
  doctestOnlyCodeBlocksMode := true,

  )
}

def mimaSettings(projectName: String) = Seq(
  mimaPreviousArtifacts := {
    /**
      * Needed in order to avoid running mima compatibility checks when compatibility series is equal to the project
      * version. Because in such case the compatibility series version does not exist and the mima checks would
      * otherwise fail.
      */
    val isFirstCompatibilitySeriesRelease = monixConnectSeries.endsWith("0")
    if (isFirstCompatibilitySeriesRelease) {
      Set.empty
    } else {
      Set("io.monix" %% projectName % monixConnectSeries)
    }
  },
  mimaBinaryIssueFilters ++= MimaFilters.allMimaFilters
)

mimaFailOnNoPrevious in ThisBuild := false

//ignores scaladoc link warnings (which are
scalacOptions in (Compile, doc) ++= Seq("-no-link-warnings")

val IT = config("it") extend Test

//=> published modules
lazy val monixConnect = (project in file("."))
  .configs(IntegrationTest, IT)
  .settings(sharedSettings())
  .settings(name := "monix-connect")
  .aggregate(dynamodb, parquet, gcs, hdfs, mongodb, redis, s3, sqs, elasticsearch, awsAuth)
  .dependsOn(dynamodb, parquet, gcs, hdfs, mongodb, redis, s3, sqs, elasticsearch, awsAuth)

lazy val dynamodb = monixConnector("dynamodb", Dependencies.DynamoDb).aggregate(awsAuth).dependsOn(awsAuth % "compile->compile;test->test")
  .settings(libraryDependencies ++= (CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((3, _)) => Seq.empty
    case _ => Seq("org.mockito" %% "mockito-scala" % Versions.Mockito % Test)
  }))

lazy val hdfs = monixConnector("hdfs", Dependencies.Hdfs)
  .settings(libraryDependencies ++= (CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((3, _)) => Seq.empty
    case _ => Seq("org.mockito" %% "mockito-scala" % Versions.Mockito % Test)
  }))


lazy val mongodb = monixConnector("mongodb", Dependencies.MongoDb, isMimaEnabled = false, isITParallelExecution = true, scala3Publish = false)
  .settings(libraryDependencies ++= (CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((3, _)) => Seq.empty
    case _ => Seq(
      "org.mongodb.scala" %% "mongo-scala-driver"           % Versions.MongoScala,
      "org.mongodb.scala" %% "mongo-scala-bson"             % Versions.MongoScala % Test,
      "org.mockito" %% "mockito-scala" % Versions.Mockito % Test cross CrossVersion.for3Use2_13)
  }))

lazy val parquet = monixConnector("parquet", Dependencies.Parquet, scala3Publish = false)
  .settings(libraryDependencies ++= (CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((3, _)) => Seq.empty
    case _ => Seq("org.mockito" %% "mockito-scala" % Versions.Mockito % Test)
  }))

val protoTestSettings = Seq(
    Compile / PB.targets := Seq(
      scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
    )
      //Compile / PB.protoSources := Seq(new File("src/test/protobuf"))
  )

lazy val redis = monixConnector("redis", Dependencies.Redis, isITParallelExecution = false)
  .settings(protoTestSettings)

lazy val s3 = monixConnector("s3", Dependencies.S3, isMimaEnabled = false, isITParallelExecution = true)
  .aggregate(awsAuth).dependsOn(awsAuth % "compile->compile;test->test")

lazy val sqs = monixConnector("sqs", Dependencies.Sqs, isMimaEnabled = false, isITParallelExecution = true)
  .aggregate(awsAuth).dependsOn(awsAuth % "compile->compile;test->test")

lazy val gcs = monixConnector("gcs", Dependencies.GCS , isITParallelExecution = false)
  .settings(libraryDependencies ++= (CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((3, _)) => Seq.empty
    case _ => Seq("org.mockito" %% "mockito-scala" % Versions.Mockito % Test)
  }))

lazy val elasticsearch =  monixConnector("elasticsearch", Dependencies.Elasticsearch, isITParallelExecution = true)

//internal
lazy val awsAuth = monixConnector("aws-auth", Dependencies.AwsAuth, isMimaEnabled = false)

def monixConnector(
                    connectorName: String,
                    projectDependencies: Seq[ModuleID],
                    isMimaEnabled: Boolean = true,
                    isITParallelExecution: Boolean = false,
                    scala3Publish: Boolean = true) = {
  Project(id = connectorName, base = file(connectorName))
    .enablePlugins(AutomateHeaderPlugin)
    .settings(name := s"monix-$connectorName",
      libraryDependencies ++= projectDependencies,
      Defaults.itSettings,
      IntegrationTest / parallelExecution := isITParallelExecution,
      IntegrationTest / testForkedParallel := isITParallelExecution
    )
    .settings(sharedSettings(scala3Publish))
    .configs(IntegrationTest, IT)
    .enablePlugins(AutomateHeaderPlugin)
    .settings(
      if(isMimaEnabled) {
        mimaSettings(s"monix-$connectorName")
      } else { Seq.empty },
      Compile / doc / sources := { if (scalaVersion.value.startsWith("3.")) Seq.empty else (Compile / doc / sources).value },
      Test / doc / sources := { if (scalaVersion.value.startsWith("3.")) Seq.empty else (Compile / doc / sources).value }
    )
}

//=> non published modules

lazy val benchmarks = monixConnector("benchmarks", Dependencies.Benchmarks, isMimaEnabled = false)
  .enablePlugins(JmhPlugin)
  .settings(skipOnPublishSettings)
  .dependsOn(parquet % "compile->compile;test->test", redis % "compile->compile;test->test", s3 % "compile->compile;test->test")
  .aggregate(parquet, redis, s3)

lazy val docs = project
  .in(file("monix-connect-docs"))
  .settings(
    moduleName := "monix-connect-docs",
    name := moduleName.value,
    sharedSettings(publishForScala3 = false),
    skipOnPublishSettings,
    mdocSettings
  )
  .enablePlugins(DocusaurusPlugin, MdocPlugin, ScalaUnidocPlugin)

lazy val skipOnPublishSettings = Seq(
  skip in publish := true,
  publishArtifact := false,
)

lazy val mdocSettings = Seq(
  scalacOptions --= Seq("-Xfatal-warnings", "-Ywarn-unused"),
  crossScalaVersions := Seq(scalaVersion.value),
  unidocProjectFilter in (ScalaUnidoc, unidoc) := inProjects(parquet, dynamodb, s3, sqs, elasticsearch, gcs, hdfs, mongodb, redis),
  target in (ScalaUnidoc, unidoc) := (baseDirectory in LocalRootProject).value / "website" / "static" / "api",
  cleanFiles += (target in (ScalaUnidoc, unidoc)).value,
  docusaurusCreateSite := docusaurusCreateSite
    .dependsOn(unidoc in Compile)
    .dependsOn(updateSiteVariables in ThisBuild)
    .value,
  docusaurusPublishGhpages :=
    docusaurusPublishGhpages
      .dependsOn(unidoc in Compile)
      .dependsOn(updateSiteVariables in ThisBuild)
      .value,
  scalacOptions in (ScalaUnidoc, unidoc) ++= Seq(
    "-doc-source-url", s"https://github.com/monix/monix-connect/tree/v${version.value}€{FILE_PATH}.scala",
    "-sourcepath", baseDirectory.in(LocalRootProject).value.getAbsolutePath,
    "-doc-title", "Monix Connect",
    "-doc-version", s"v${version.value}",
    "-groups"
  ),
  // Exclude monix.*.internal from ScalaDoc
  sources in (ScalaUnidoc, unidoc) ~= (_ filterNot { file =>
    // Exclude protobuf generated files
    file.getCanonicalPath.contains("/src_managed/main/monix/connect/")
    file.getCanonicalPath.contains("monix-connect/redis/target/scala-2.12/src_managed")
    file.getCanonicalPath.contains("monix-connect/redis/target/scala-2.13/src_managed")
  }),
)

def minorVersion(version: String): String = {
  val (major, minor) =
    CrossVersion.partialVersion(version).get
  s"$major.$minor"
}


val updateSiteVariables = taskKey[Unit]("Update site variables")
updateSiteVariables in ThisBuild := {
  val file =
    (baseDirectory in LocalRootProject).value / "website" / "variables.js"

  val variables =
    Map[String, String](
      "organization" -> (organization in LocalRootProject).value,
      "coreModuleName" -> (moduleName in monixConnect).value,
      "latestVersion" -> version.value,
      "scalaPublishVersions" -> {
        val minorVersions = (crossScalaVersions in monixConnect).value.map(minorVersion)
        if (minorVersions.size <= 2) minorVersions.mkString(" and ")
        else minorVersions.init.mkString(", ") ++ " and " ++ minorVersions.last
      }
    )

  val fileHeader =
    "// Generated by sbt. Do not edit directly."

  val fileContents =
    variables.toList
      .sortBy { case (key, _) => key }
      .map { case (key, value) => s"  $key: '$value'" }
      .mkString(s"$fileHeader\nmodule.exports = {\n", ",\n", "\n};\n")

  IO.write(file, fileContents)
}

