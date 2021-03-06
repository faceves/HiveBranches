import Dependencies._

ThisBuild / scalaVersion     := "2.13.5"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "HiveBranches",
    libraryDependencies += scalaTest % Test,

  //https://mvnrepository.com/artifact/org.apache.hive/hive-jdbc
  libraryDependencies += "org.apache.hive" % "hive-jdbc" % "2.3.8",
  // https://mvnrepository.com/artifact/org.apache.hive/hive-metastore
  libraryDependencies += "org.apache.hive" % "hive-metastore" % "2.3.8"
    
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
