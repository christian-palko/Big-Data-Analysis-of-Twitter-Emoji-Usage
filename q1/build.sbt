import Dependencies._

ThisBuild / scalaVersion := "2.11.12"
ThisBuild / version := "0.1"
ThisBuild / organization := "com.revature"
ThisBuild / organizationName := "revature"

lazy val root = (project in file("."))
  .settings(
    name := "questionone",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7" % "provided",
    // https://mvnrepository.com/artifact/org.apache.httpcomponents/httpclient
    libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.12",
    // https://mvnrepository.com/artifact/commons-io/commons-io
    libraryDependencies += "commons-io" % "commons-io" % "2.8.0"
  )
  