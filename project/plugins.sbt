addSbtPlugin("org.scalariform"      % "sbt-scalariform"     % "1.8.2")
addSbtPlugin("com.typesafe.sbt"     % "sbt-native-packager" % "1.3.4")
addSbtPlugin("com.thesamet"         % "sbt-protoc"          % "0.99.27")
//addSbtPlugin("com.typesafe.sbt"     % "sbt-twirl"           % "1.5.0")
addSbtPlugin("com.typesafe.play" % "sbt-plugin" % "2.6.25")
addCompilerPlugin("org.scalamacros" % "paradise"            % "2.1.1" cross CrossVersion.full)

resolvers += Resolver.bintrayRepo("beyondthelines", "maven")

libraryDependencies ++= Seq(
  "com.thesamet.scalapb" %% "compilerplugin" % "0.7.0",
  "beyondthelines"       %% "grpcmonixgenerator" % "0.0.7"
)
