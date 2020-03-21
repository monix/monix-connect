import sbt._

object Dependencies {

  object DependencyVersions {
    val PureConfig = "0.10.1"
    val Monix = "3.1.0"
    val Circe = "0.11.1"
    val TypesafeConfig = "1.3.2"

    val Log4jScala = "11.0"
    val Log4j = "2.10.0"
    val ScalaLogging = "3.9.2"

    val Scalatest = "3.0.4"
    val Scalacheck = "1.13.5"
    val Mockito = "2.18.3"
  }

  val S3Main = Seq(
    "io.monix" %% "monix-reactive" % DependencyVersions.Monix,
    "com.github.pureconfig"     %% "pureconfig"            % DependencyVersions.PureConfig,
    "com.typesafe"              % "config"                 % DependencyVersions.TypesafeConfig,
  )

  val TestDependencies = Seq(
    "org.scalatest"             %% "scalatest"             % DependencyVersions.Scalatest,
    "org.scalacheck"            %% "scalacheck"            % DependencyVersions.Scalacheck,
    "org.mockito"               %  "mockito-core"          % DependencyVersions.Mockito,
  )

  val S3 = S3Main ++ TestDependencies.map(_ % Test) ++ TestDependencies.map(_ % IntegrationTest)
}
