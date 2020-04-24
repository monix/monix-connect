import com.typesafe.sbt.GitVersioning
import sbt.Keys.version
// For getting Scoverage out of the generated POM
import scala.xml.Elem
import scala.xml.transform.{RewriteRule, RuleTransformer}

lazy val sharedSettings = warnUnusedImport ++ Seq(
  organization := "io.monix",
  scalaVersion := "2.13.1",
  crossScalaVersions := Seq("2.11.12", "2.12.10", "2.13.1"),

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

  scalacOptions ++= (CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, v)) if v <= 12 =>
      Seq(
        // possibly deprecated options
        "-Ywarn-inaccessible",
        // absolutely necessary for Iterant
        "-Ypartial-unification",
      )
    case _ =>
      Seq(
        "-Ymacro-annotations",
      )
  }),

  // Linter
  scalacOptions ++= Seq(
    // Turns all warnings into errors ;-)
    "-Xfatal-warnings",
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
  scalacOptions ++= (CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, majorVersion)) if majorVersion <= 12 =>
      Seq(
        "-Xlint:inaccessible", // warn about inaccessible types in method signatures
        "-Xlint:by-name-right-associative", // By-name parameter of right associative operator
        "-Xlint:unsound-match" // Pattern match may not be typesafe
      )
    case _ =>
      Seq.empty
  }),

  // Turning off fatal warnings for ScalaDoc, otherwise we can't release.
  scalacOptions in (Compile, doc) ~= (_ filterNot (_ == "-Xfatal-warnings")),

  // For working with partially-applied types
  addCompilerPlugin("org.typelevel" % "kind-projector" % "0.11.0" cross CrossVersion.full),

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

  resolvers ++= Seq(
    "Typesafe Releases" at "https://repo.typesafe.com/typesafe/releases",
    Resolver.sonatypeRepo("releases")
  ),

  // https://github.com/sbt/sbt/issues/2654
  incOptions := incOptions.value.withLogRecompileOnMacro(false),

  // -- Settings meant for deployment on oss.sonatype.org
  sonatypeProfileName := organization.value,

  credentials += Credentials(
    "Sonatype Nexus Repository Manager",
    "oss.sonatype.org",
    sys.env.getOrElse("SONATYPE_USER", ""),
    sys.env.getOrElse("SONATYPE_PASS", "")
  ),

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

  licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
  homepage := Some(url("https://monix.io")),
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

  scmInfo := Some(
    ScmInfo(
      url("https://github.com/monix/monix"),
      "scm:git@github.com:monix/monix.git"
    )),

  developers := List(
    Developer(
      id="paualarco",
      name="Pau Alarco Nedelcu",
      email="noreply@alexn.org",
      url=url("https://github.com/paualarco")
    ))
)

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
