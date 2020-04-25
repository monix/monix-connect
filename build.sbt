import com.typesafe.sbt.GitVersioning
import sbt.Keys.version
// For getting Scoverage out of the generated POM
import scala.xml.Elem
import scala.xml.transform.{RewriteRule, RuleTransformer}

// The Monix Connect version with which we must keep binary compatibility.
// https://github.com/typesafehub/migration-manager/wiki/Sbt-plugin
val monixSeries = "0.0.0"

//lazy val connectors = akka :: common :: dynamoDB :: hdfs :: parquet :: redis :: s3 :: Nil

lazy val doNotPublishArtifact = Seq(
  publishArtifact := false,
  publishArtifact in (Compile, packageDoc) := false,
  publishArtifact in (Compile, packageSrc) := false,
  publishArtifact in (Compile, packageBin) := false
)

lazy val sharedSettings = Seq(
  organization := "io.monix",
  scalaVersion := "2.13.1",
  version      := Version.version,
  crossScalaVersions := Seq("2.12.10", "2.13.1"),
  scalafmtOnCompile := true,
  scalacOptions ++= Seq(
    // warnings
    "-unchecked", // able additional warnings where generated code depends on assumptions
    "-deprecation", // emit warning for usages of deprecated APIs
    "-feature", // emit warning usages of features that should be imported explicitly
    // Features enabled by default
    "-language:higherKinds",
    "-language:implicitConversions",
    "-language:experimental.macros",
  ),
  //warnUnusedImports
  //scalacOptions ++= Seq("-Ywarn-unused-import"),
  scalacOptions in (Compile, console) --= Seq("-Ywarn-unused-import", "-Ywarn-unused:imports"),
  scalacOptions in Test --= Seq("-Ywarn-unused-import", "-Ywarn-unused:imports"),
  // Linter
  scalacOptions ++= Seq(
    // Turns all warnings into errors ;-)
    //"-Xfatal-warnings", todo activate flag
    // Enables linter options
    "-Xlint:adapted-args", // warn if an argument list is modified to match the receiver
    "-Xlint:nullary-unit", // warn when nullary methods return Unit
    "-Xlint:nullary-override", // warn when non-nullary `def f()' overrides nullary `def f'
    "-Xlint:infer-any", // warn when a type argument is inferred to be `Any`
    "-Xlint:missing-interpolator", // a string literal appears to be missing an interpolator id
    "-Xlint:doc-detached", // a ScalaDoc comment appears to be detached from its element
    "-Xlint:private-shadow", // a private field (or class parameter) shadows a superclass field
    "-Xlint:type-parameter-shadow", // a local type parameter shadows a type already in scope
    "-Xlint:poly-implicit-overload", // parameterized overloaded implicit methods are not visible as view bounds
    "-Xlint:option-implicit", // Option.apply used implicit view
    "-Xlint:delayedinit-select", // Selecting member of DelayedInit
    "-Xlint:package-object-classes", // Class or object defined in package object
  ),

  // Turning off fatal warnings for ScalaDoc, otherwise we can't release.
  //scalacOptions in (Compile, doc) ~= (_ filterNot (_ == "-Xfatal-warnings")),

  // ScalaDoc settings
  autoAPIMappings := true,
  scalacOptions in ThisBuild ++= Seq(
    // Note, this is used by the doc-source-url feature to determine the
    // relative path of a given source file. If it's not a prefix of a the
    // absolute path of the source file, the absolute path of that file
    // will be put into the FILE_SOURCE variable, which is
    // definitely not what we want.
    "-sourcepath", file(".").getAbsolutePath.replaceAll("[.]$", "")
  ),

  parallelExecution in Test := false,
  parallelExecution in IntegrationTest := false,
  parallelExecution in ThisBuild := false,
  testForkedParallel in Test := false,
  testForkedParallel in IntegrationTest := false,
  testForkedParallel in ThisBuild := false,
  concurrentRestrictions in Global += Tags.limit(Tags.Test, 1),

  logBuffered in Test := false,
  logBuffered in IntegrationTest := false,

  // https://github.com/sbt/sbt/issues/2654
  incOptions := incOptions.value.withLogRecompileOnMacro(false),

  // -- Settings meant for deployment on oss.sonatype.org
  /*sonatypeProfileName := organization.value, todo add sonatype
  credentials += Credentials(
    "Sonatype Nexus Repository Manager",
    "oss.sonatype.org",
    sys.env.getOrElse("SONATYPE_USER", ""),
    sys.env.getOrElse("SONATYPE_PASS", "")
  ),*/

  publishMavenStyle := true,
  publishTo := Some(
    if (isSnapshot.value)
      Opts.resolver.sonatypeSnapshots
    else
      Opts.resolver.sonatypeStaging
  ),

  isSnapshot := version.value endsWith "SNAPSHOT",
  publishArtifact in Test := false,
  pomIncludeRepository := { _ => false }, // removes optional dependencies
  // For evicting Scoverage out of the generated POM
  // See: https://github.com/scoverage/sbt-scoverage/issues/153
  pomPostProcess := { (node: xml.Node) =>
    new RuleTransformer(new RewriteRule {
      override def transform(node: xml.Node): Seq[xml.Node] = node match {
        case e: Elem
          if e.label == "dependency" && e.child.exists(child => child.label == "groupId" && child.text == "org.scoverage") => Nil
        case _ => Seq(node)
      }
    }).transform(node).head
  },

  headerLicense := None,

  licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),

  //homepage := Some(url("https://monix.io")), //todo homepage settings

  headerLicense := Some(HeaderLicense.Custom(
    """|Copyright (c) 2014-2020 by The Monix Project Developers.
       |See the project homepage at: https://monix.io
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
      .stripMargin)),

  //todo add scm

  developers := List(
    Developer(
      id="paualarco",
      name="Pau Alarcon",
      email="pau.alarcon.b@gmail.com",
      url=url("https://github.com/paualarco")
    )),
    doctestTestFramework := DoctestTestFramework.ScalaTest,
    doctestTestFramework := DoctestTestFramework.ScalaCheck,
    doctestOnlyCodeBlocksMode := true
)

lazy val unidocSettings = Seq(
  //unidocProjectFilter in (ScalaUnidoc, unidoc) :=
    //inProjects(akka, common, dynamoDB, hdfs, s3),
  scalacOptions in (ScalaUnidoc, unidoc) +=
    "-Xfatal-warnings",
  scalacOptions in (ScalaUnidoc, unidoc) --=
    Seq("-Ywarn-unused-import", "-Ywarn-unused:imports"),
  scalacOptions in (ScalaUnidoc, unidoc) ++=
    Opts.doc.title(s"Monix Connect"),
  scalacOptions in (ScalaUnidoc, unidoc) ++=
    Opts.doc.sourceUrl(s"https://github.com/monix/monix-connect/tree/v${version.value}€{FILE_PATH}.scala"),
  //scalacOptions in (ScalaUnidoc, unidoc) ++=
  //  Seq("-doc-root-content", file("rootdoc.txt").getAbsolutePath), //todo check usage
  scalacOptions in (ScalaUnidoc, unidoc) ++=
    Opts.doc.version(s"${version.value}")
)

def profile: Project => Project = pr => {
  val withCoverage = sys.env.getOrElse("SBT_PROFILE", "") match {
    case "coverage" => pr
    case _ => pr.disablePlugins(scoverage.ScoverageSbtPlugin)
  }
  withCoverage.enablePlugins(AutomateHeaderPlugin)
}

val IT = config("it") extend Test

lazy val monix = (project in file("."))
  .configs(IntegrationTest, IT)
  .settings(sharedSettings)
  .settings(name := "monix-connect")
  .aggregate(akka, redis)
  .dependsOn(akka, redis)
  //.aggregate(akka, common, dynamoDB, hdfs, parquet, s3)
  //.dependsOn(akka, common, dynamoDB, hdfs, parquet, s3)

lazy val redis = monixConnector("redis", Dependencies.Redis)

lazy val akka = monixConnector("akka", Dependencies.Akka)
/*
lazy val common = monixConnector("common", Dependencies.Common)

lazy val dynamoDB = monixConnector("dynamodb", Dependencies.DynamoDb)
  .dependsOn(common % "compile->compile; test->test")

lazy val hdfs = monixConnector("hdfs", Dependencies.Hdfs)

val scalaPBSettings = Seq(
  PB.targets in Compile := Seq(
    scalapb.gen() -> (sourceManaged in Compile).value
  ),
  PB.targets in Compile := Seq(
    scalapb.gen(javaConversions=true) -> (sourceManaged in Compile).value,
    PB.gens.java -> (sourceManaged in Compile).value
  )
)
lazy val parquet = monixConnector("parquet", Dependencies.Parquet, scalaPBSettings)

lazy val s3 = monixConnector("s3", Dependencies.S3)
*/

def monixConnector(connectorName: String, projectDependencies: Seq[ModuleID], additionalSettings: sbt.Def.SettingsDefinition*): Project =
  Project(id = connectorName, base = file(connectorName))
    .enablePlugins(AutomateHeaderPlugin)
    .settings(
      name := s"monix-$connectorName",
      libraryDependencies ++= projectDependencies,
      Defaults.itSettings)
    .settings(sharedSettings)
    .settings(additionalSettings: _*)
    .configure(profile)
    .configs(IntegrationTest, IT)

//todo add release settings
