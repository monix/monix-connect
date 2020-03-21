import sbt._

object Dependencies {

  object DependencyVersions {
    val PureConfig = "0.10.1"
    val Monix = "2.3.3"
    val Circe = "0.11.1"
    val Http4s = "0.20.10"
    val AkkaHttp = "10.1.11"
    val Akka = "2.5.26"
    val TypesafeConfig = "1.3.2"

    val Log4jScala = "11.0"
    val Log4j = "2.10.0"
    val ScalaLogging = "3.9.2"

    val Scalatest = "3.0.4"
    val Scalacheck = "1.13.5"
    val Mockito = "2.18.3"
  }

  val FrontendDependencies: Seq[ModuleID] = Seq(
    "com.typesafe.play"         %% "twirl-api"             % "1.5.0",
    "org.webjars" % "bootstrap" % "3.3.6"
  )

  val MasterDependencies: Seq[ModuleID] = Seq(
    "com.github.pureconfig"     %% "pureconfig"            % DependencyVersions.PureConfig,
    "com.typesafe"              % "config"                 % DependencyVersions.TypesafeConfig,
    "org.scalacheck"            %% "scalacheck"            % DependencyVersions.Scalacheck,
  )

  private val TestDependencies = Seq(
    "com.typesafe.play"         %% "twirl-api"             % "1.5.0",
    "org.scalatest"             %% "scalatest"             % DependencyVersions.Scalatest,
    "org.scalacheck"            %% "scalacheck"            % DependencyVersions.Scalacheck,
    "org.mockito"               %  "mockito-core"          % DependencyVersions.Mockito,
    "org.scalatestplus.play" %% "scalatestplus-play" % "5.0.0"
  ).map( _ % Test)

  val MasterSlaveDependencies: Seq[ModuleID] =Seq(
    "io.grpc"                   % "grpc-netty"             % scalapb.compiler.Version.grpcJavaVersion,
    "com.thesamet.scalapb"      %% "scalapb-runtime-grpc"  % scalapb.compiler.Version.scalapbVersion,
    "com.thesamet.scalapb"      %% "scalapb-runtime"       % scalapb.compiler.Version.scalapbVersion % "protobuf",
    "beyondthelines"            %% "grpcmonixgenerator"    % "0.0.7",
    "com.thesamet.scalapb"      %% "compilerplugin"        % "0.7.0",
    "beyondthelines"            %% "grpcmonixruntime"      % "0.0.7",
    "org.apache.logging.log4j"  %% "log4j-api-scala"       % DependencyVersions.Log4jScala,
    "org.apache.logging.log4j"  % "log4j-api"              % DependencyVersions.Log4j,
    "org.apache.logging.log4j"  % "log4j-core"             % DependencyVersions.Log4j,
    "org.apache.logging.log4j"  % "log4j-slf4j-impl"       % DependencyVersions.Log4j,
    // "io.monix"                  %% "monix"                 % DependencyVersions.Monix,
    "org.http4s"                %% "http4s-server"         % DependencyVersions.Http4s,
    "org.http4s"                %% "http4s-core"           % DependencyVersions.Http4s,
    "org.http4s"                %% "http4s-dsl"            % DependencyVersions.Http4s,
    "org.http4s"                %% "http4s-circe"          % DependencyVersions.Http4s,
    "org.http4s"                %% "http4s-blaze-server"   % DependencyVersions.Http4s,
    "org.http4s"                %% "http4s-twirl"          % DependencyVersions.Http4s,
    "io.circe"                  %% "circe-core"            % DependencyVersions.Circe,
    "io.circe"                  %% "circe-generic"         % DependencyVersions.Circe,
    "io.circe"                  %% "circe-parser"          % DependencyVersions.Circe,
    "org.scalacheck"            %% "scalacheck"            % DependencyVersions.Scalacheck,
    "com.typesafe.scala-logging"    %% "scala-logging" % DependencyVersions.ScalaLogging
  )

  val CommonProjectDependencies: Seq[ModuleID] = MasterSlaveDependencies ++ TestDependencies

}
