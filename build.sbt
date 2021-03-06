organization := "io.github.andreas-schroeder"

scalaVersion := "2.13.3"

val akkaVersion = "2.6.6"
val akkaHttpVersion = "10.2.0"

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

inThisBuild(List(
  organization := "io.github.andreas-schroeder",
  homepage := Some(url("https://github.com/andreas-schroeder/akka-http-concurrency-limits")),
  licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
  developers := List(
    Developer(
      "andreas-schroeder",
      "Andreas Schroeder",
      sys.env.getOrElse("E_MAIL", "john.doe@example.com"),
      url("https://github.com/andreas-schroeder")
    )
  )
))