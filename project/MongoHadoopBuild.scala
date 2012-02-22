import sbt._
import Keys._
import Reference._
import sbtassembly.Plugin._
import AssemblyKeys._

object MongoHadoopBuild extends Build {

  lazy val buildSettings = Seq(
    version := "1.0.0-rc1-SNAPSHOT",
    crossScalaVersions := Nil,
    crossPaths := false,
    organization := "org.mongodb"
  )

  /** The version of Hadoop to build against. */
  lazy val hadoopRelease = SettingKey[String]("hadoop-release", "Hadoop Target Release Distro/Version")


  private val stockPig = "0.9.2"
  private val cdh3Rel = "cdh3u3"
  private val cdh3Hadoop = "0.20.2-%s".format(cdh3Rel) // current "base" version they patch against
  private val cdh3Pig = "0.8.1-%s".format(cdh3Rel)
  private val cdh4Rel = "cdh4b1"
  private val cdh4YarnHadoop = "0.23.0-%s".format(cdh4Rel) // current "base" version they patch against
  private val cdh4CoreHadoop = "0.23.0-mr1-%s".format(cdh4Rel)
  private val cdh4Pig = "0.9.2-%s".format(cdh4Rel)

  private val coreHadoopMap = Map("0.20" -> hadoopDependencies("0.20.205.0", false, stockPig),
                                  "0.20.x" -> hadoopDependencies("0.20.205.0", false, stockPig),
                                  "0.21" -> hadoopDependencies("0.21.0", true, stockPig),
                                  "0.21.x" -> hadoopDependencies("0.21.0", true, stockPig), 
                                  "0.23" -> hadoopDependencies(cdh4CoreHadoop, true, cdh4Pig, Some(cdh4YarnHadoop)),
                                  "0.23.x" -> hadoopDependencies(cdh4CoreHadoop, true, cdh4Pig, Some(cdh4YarnHadoop)),
                                  "cdh4" -> hadoopDependencies(cdh4CoreHadoop, true, cdh4Pig, Some(cdh4YarnHadoop)),
                                  "cdh3" -> hadoopDependencies(cdh3Hadoop, true, cdh3Pig),
                                  "1.0" -> hadoopDependencies("1.0.0", false, stockPig),
                                  "1.0.x" -> hadoopDependencies("1.0.0", false, stockPig),
                                  "default" -> hadoopDependencies("1.0.0", false, stockPig)
                                 )

  lazy val root = Project( id = "mongo-hadoop", 
                          base = file("."),
                          settings = dependentSettings ) aggregate(core, flume, pig)

  lazy val core = Project( id = "mongo-hadoop-core", 
                           base = file("core"), 
                           settings = coreSettings )


  lazy val pig = Project( id = "mongo-hadoop-pig",
                          base = file("pig"),
                          settings = pigSettings ) dependsOn( core )

  lazy val streaming = Project( id = "mongo-hadoop-streaming", 
                                base = file("streaming"), 
                                settings = streamingSettings ) dependsOn( core )

  lazy val flume = Project( id = "mongo-flume",
                            base = file("flume"),
                            settings = flumeSettings ) 



  lazy val baseSettings = Defaults.defaultSettings ++ buildSettings ++ Seq( 
    resolvers ++= Seq(Resolvers.mitSimileRepo, Resolvers.clouderaRepo, Resolvers.mavenOrgRepo, Resolvers.sonatypeRels),

    libraryDependencies <<= (libraryDependencies) { deps =>
      
      val scala: ModuleID = deps.find { x => x.name == "scala-library" }.map ( y => 
        y.copy(configurations = Some("test"))
      ).get


      val newDeps = deps.filterNot { x => x.name == "scala-library" }

      newDeps :+ scala
    }

  )

  /** Settings that are dependent on a hadoop version */
  lazy val dependentSettings = baseSettings ++ Seq(    
    moduleName <<= (hadoopRelease, moduleName) { (hr, mod) =>
      if (hr == "default") 
        mod
      else {
        val rel = coreHadoopMap.getOrElse(hr, 
                      sys.error("Hadoop Release '%s' is an invalid/unsupported release. " +
                                " Valid entries are in %s".format(hr, coreHadoopMap.keySet))
                      )._3

        
        if (hr == "cdh3")
          "%s_%s".format(mod, cdh3Rel)
        else if (hr == "cdh4")
          "%s_%s".format(mod, cdh4Rel)
        else 
          "%s_%s".format(mod, rel)
      }
  })

  lazy val parentSettings = baseSettings ++ Seq( 
    publishArtifact := false
  )

  lazy val flumeSettings = baseSettings ++ Seq(
    libraryDependencies ++= Seq(Dependencies.mongoJavaDriver, Dependencies.flume)
  )

  lazy val streamingSettings = dependentSettings ++ assemblySettings ++ Seq( 
    mainClass in assembly := Some("com.mongodb.hadoop.streaming.MongoStreamJob"),
    excludedJars in assembly <<= (fullClasspath in assembly) map ( cp => 
      cp filterNot { x =>
        x.data.getName.startsWith("hadoop-streaming") || x.data.getName.startsWith("mongo-java-driver") /*||
        x.data.getName.startsWith("mongo-hadoop-core") */
      }
    ),
    libraryDependencies <++= (scalaVersion, libraryDependencies, hadoopRelease) { (sv, deps, hr: String) => 

    val streamingDeps = coreHadoopMap.getOrElse(hr, sys.error("Hadoop Release '%s' is an invalid/unsupported release. Valid entries are in %s".format(hr, coreHadoopMap.keySet)))
      streamingDeps._1.getOrElse(() => Seq.empty[ModuleID])()
    },
    skip in Compile <<= hadoopRelease.map(hr => {
      val skip = !coreHadoopMap.getOrElse(hr, 
                    sys.error("Hadoop Release '%s' is an invalid/unsupported release. " +
                              " Valid entries are in %s".format(hr, coreHadoopMap.keySet))
                    )._1.isDefined
      if (skip) System.err.println("*** Will not compile Hadoop Streaming, which is unsupported in this build of Hadoop")

      skip
    }),
    publishArtifact <<= (hadoopRelease) (hr => {
      !coreHadoopMap.getOrElse(hr, sys.error("Hadoop Release '%s' is an invalid/unsupported release. " +
                              " Valid entries are in %s".format(hr, coreHadoopMap.keySet)))._1.isDefined
    })

  ) 

  val pigSettings = dependentSettings ++ Seq( 
    resolvers ++= Seq(Resolvers.hypobytes), /** Seems to have thrift deps I need*/
    libraryDependencies <++= (scalaVersion, libraryDependencies, hadoopRelease) { (sv, deps, hr: String) => 

      val hadoopDeps = coreHadoopMap.getOrElse(hr, sys.error("Hadoop Release '%s' is an invalid/unsupported release. Valid entries are in %s".format(hr, coreHadoopMap.keySet)))
      hadoopDeps._4()
    }
  )

  val coreSettings = dependentSettings ++ Seq( 
    libraryDependencies ++= Seq(Dependencies.mongoJavaDriver, Dependencies.junit),
    libraryDependencies <++= (scalaVersion, libraryDependencies, hadoopRelease) { (sv, deps, hr: String) => 

      val hadoopDeps = coreHadoopMap.getOrElse(hr, sys.error("Hadoop Release '%s' is an invalid/unsupported release. Valid entries are in %s".format(hr, coreHadoopMap.keySet)))
      hadoopDeps._2()
    }, 
    libraryDependencies <<= (scalaVersion, libraryDependencies) { (sv, deps) =>
      val versionMap = Map("2.8.0" -> ("specs2_2.8.0", "1.5"),
                           "2.8.1" -> ("specs2_2.8.1", "1.5"),
                           "2.9.0" -> ("specs2_2.9.0", "1.7.1"),
                           "2.9.0-1" -> ("specs2_2.9.0", "1.7.1"),
                           "2.9.1" -> ("specs2_2.9.1", "1.7.1"))
      val tuple = versionMap.getOrElse(sv, sys.error("Unsupported Scala version for Specs2"))
      deps :+ ("org.specs2" % tuple._1 % tuple._2 % "test")
    },
    autoCompilerPlugins := true,
    parallelExecution in Test := true,
    testFrameworks += TestFrameworks.Specs2
  )



  def hadoopDependencies(hadoopVersion: String, useStreaming: Boolean, pigVersion: String, altStreamingVer: Option[String] = None): (Option[() => Seq[ModuleID]], () => Seq[ModuleID], String, () => Seq[ModuleID]) = {
      (if (useStreaming) Some(streamingDependency(altStreamingVer.getOrElse(hadoopVersion))) else None, () => {
      println("*** Adding Hadoop Dependencies for Hadoop '%s'".format(altStreamingVer.getOrElse(hadoopVersion)))

      val dep = Seq("org.apache.hadoop" % "hadoop-core" % hadoopVersion)
      if (altStreamingVer.isDefined)
        dep ++ Seq("org.apache.hadoop" % "hadoop-common" % altStreamingVer.get)
      else 
        dep
      }, hadoopVersion, pigDependency(pigVersion))
  }

  def pigDependency(pigVersion: String): () => Seq[ModuleID] = {
    () => {
      println("*** Adding Pig Dependency for Version '%s'".format(pigVersion))

      Seq(
        "org.apache.pig" % "pig" % pigVersion      
      )
    }
  }

  def streamingDependency(hadoopVersion: String): () => Seq[ModuleID] = {
    () => {
      println("Enabling build of Hadoop Streaming.")
      Seq("org.apache.hadoop" % "hadoop-streaming" % hadoopVersion) 
    }
  }


}

object Resolvers {
  val scalaToolsSnapshots = "snapshots" at "http://scala-tools.org/repo-snapshots"
  val scalaToolsReleases  = "releases" at "http://scala-tools.org/repo-releases"
  val sonatypeSnaps = "snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
  val sonatypeRels = "releases" at "https://oss.sonatype.org/content/repositories/releases"
  val clouderaRepo = "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
  val mitSimileRepo = "Simile Repo at MIT" at "http://simile.mit.edu/maven"
  val mavenOrgRepo = "Maven.Org Repository" at "http://repo1.maven.org/maven2/org/"
  /** Seems to have thrift deps I need*/
  val hypobytes = "Hypobytes" at "https://hypobytes.com/maven/content/groups/public"
}

object Dependencies {
  val mongoJavaDriver = "org.mongodb" % "mongo-java-driver" % "2.7.3"
  val junit = "junit" % "junit" % "4.10" % "test"
  val flume = "com.cloudera" % "flume-core" % "0.9.4-cdh3u3"
}

// vim: set ts=2 sw=2 sts=2 et:
