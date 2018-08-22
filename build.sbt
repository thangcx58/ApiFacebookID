name := "cassandra-spark-akka-http"

version := "0.1"

scalaVersion := "2.11.8"

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"
libraryDependencies += "datastax" % "spark-cassandra-connector" % "2.3.0-s_2.11"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.3.1",
    "org.apache.spark" %% "spark-sql" % "2.3.1",

    "com.typesafe.akka" %% "akka-http" % "10.1.3",
    "com.typesafe.akka" %% "akka-stream" % "2.5.13",

    "com.typesafe.play" %% "play-json" % "2.6.7"
)
