import Dependencies._

ThisBuild / scalaVersion     := "2.13.0"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "io.github.antivanov"
ThisBuild / organizationName := "antivanov"

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(
    name := "athena-client",
    Defaults.itSettings,
    libraryDependencies ++= Seq(

      scalaTest % "it,test",
      "software.amazon.awssdk" % "athena" % "2.7.26",
      "software.amazon.awssdk" % "s3" % "2.9.23" % "it,test",
      "org.mockito" % "mockito-core" % "2.10.0" % "test" +
        ""
    )
  )