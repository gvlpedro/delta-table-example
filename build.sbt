ThisBuild / version := "0.1.0-SNAPSHOT"
val commonVersion = "0.4.0"

val scalaVersion212 = "2.12.14"
lazy val root = (project in file("."))
  .settings(
    scalaVersion := scalaVersion212,
    useCoursier := false,
    name := "delta-test",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.2.2" % Compile,
      "org.apache.spark" %% "spark-hive" % "3.2.2" % Compile, // ERROR3
      "io.delta" %% "delta-core" % "2.1.0" % Compile
    )
  )