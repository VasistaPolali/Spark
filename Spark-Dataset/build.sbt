val commonSettings = Seq(
  name := "data-engineer-challenge",
  version := "0.1" ,
  scalaVersion := "2.11.8",
  fork in run := true,
  resolvers += "Maven2" at "http://repo.maven.apache.org/maven2/"
)

val etl = (project in file("etl"))
  .settings(commonSettings: _*)
  .settings(name += "-etl")
  .settings(
    libraryDependencies ++=
      Seq(
      "org.apache.spark" %% "spark-core" % "2.3.1",
      "org.apache.spark" %% "spark-sql" % "2.3.1",
        "org.scalatest" %% "scalatest" % "3.0.1-SNAP1" % "test"


    )
)
