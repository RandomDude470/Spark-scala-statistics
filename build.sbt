ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "newSparkFinal"
  )
libraryDependencies += "io.github.cibotech" %% "evilplot" % "0.9.0"
libraryDependencies += "io.github.cibotech" %% "evilplot-repl" % "0.9.0"