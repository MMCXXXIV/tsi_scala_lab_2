ThisBuild / scalaVersion := "2.13.16"

lazy val root = (project in file("."))
  .settings(
    name := "scala_lab_2",
    version := "1.0.0",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "4.0.1",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    ),
    Test / fork := true,
    Test / parallelExecution := false
  )
