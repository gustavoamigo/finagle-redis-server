name := "finagle-redis-server"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(

  "com.twitter" %% "util-core" % "6.40.0",
  "com.twitter" %% "finagle-core" % "6.41.0",
  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

