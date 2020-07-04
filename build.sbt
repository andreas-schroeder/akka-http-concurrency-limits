version := "0.0.1"

organization := "com.github.andreas-schroeder"

scalaVersion := "2.13.3"

val akkaVersion = "2.6.6"
val akkaHttpVersion = "10.1.12"

libraryDependencies ++= Seq(
  "com.netflix.concurrency-limits" % "concurrency-limits-core" % "0.3.6",
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "org.scalatest" %% "scalatest" % "3.2.0" % Test,
  "org.mockito" %% "mockito-scala-scalatest" % "1.14.8" % Test,
  "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpVersion % Test,
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test
)

