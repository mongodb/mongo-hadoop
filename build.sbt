javacOptions += "-g"

name := "mongo-hadoop"

organization := "org.mongodb"

seq(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)


hadoopRelease in ThisBuild := "0.20"


