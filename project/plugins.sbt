addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.3.2")
addSbtPlugin("org.scalameta"       % "sbt-mdoc"        % "2.3.7")
addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.25")
addSbtPlugin("com.github.tkawachi"  % "sbt-doctest"     % "0.10.0")
addSbtPlugin("com.typesafe"         % "sbt-mima-plugin" % "1.1.1")
addSbtPlugin("com.github.sbt"         % "sbt-unidoc"      % "0.5.0")
addSbtPlugin("de.heikoseeberger"    % "sbt-header"      % "5.7.0")
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.5.10")
addSbtPlugin("pl.project13.scala"   % "sbt-jmh"         % "0.4.3")
addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.3")
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.11"
addSbtPlugin("ch.epfl.scala" % "sbt-scala3-migrate" % "0.5.1")