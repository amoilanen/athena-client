import Dependencies._

ThisBuild / scalaVersion     := "2.13.0"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "io.github.antivanov"
ThisBuild / organizationName := "antivanov"

lazy val root = (project in file("."))
  .settings(
    name := "athena-client",
    libraryDependencies ++= Seq(
      scalaTest % Test,
      "software.amazon.awssdk" % "athena" % "2.7.26"
    )
  )