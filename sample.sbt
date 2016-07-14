name := "sparkgot"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1" % "provided"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.0-M2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1" % "provided"