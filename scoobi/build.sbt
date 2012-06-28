name := "mongo-scoobi"

version := "0.1"

scalaVersion := "2.9.2"

libraryDependencies += "com.nicta" %% "scoobi" % "0.4.0" % "provided"

libraryDependencies += "org.mongodb" %% "casbah" % "2.4.0"

libraryDependencies += "org.mongodb" % "mongo-hadoop-core" % "1.0.0"

scalacOptions ++= Seq("-Ydependent-method-types", "-deprecation")

mainClass := Some("com.mongodb.hadoop.scoobi.test.TestJob")

resolvers ++= Seq("Cloudera Maven Repository" at "https://repository.cloudera.com/content/repositories/releases/",
              "Packaged Avro" at "http://nicta.github.com/scoobi/releases/", "OSS Sonatype" at "http://oss.sonatype.org/content/repositories/releases")
