import sbt._
import Keys._
import Reference._

object MongoHadoopBuild extends Build {

  lazy val buildSettings = Seq(
    version := "1.0.0-rc0",
    crossScalaVersions := Nil,
    crossPaths := false
  )

  /** The version of Hadoop to build against. */
  lazy val hadoopRelease = SettingKey[String]("hadoop-release", "Hadoop Target Release Distro/Version")


  private val stockPig = "0.9.1"
  private val cdhRel = "cdh3u3"
  private val cdhHadoop = "0.20.2-%s".format(cdhRel) // current "base" version they patch against
  private val cdhPig = "0.8.1-%s".format(cdhRel)

  private val coreHadoopMap = Map("0.20" -> hadoopDependency("0.20.205.0", false),
                                  "0.20.x" -> hadoopDependency("0.20.205.0", false),
                                  "0.21" -> hadoopDependency("0.21.0", true),
                                  "0.21.x" -> hadoopDependency("0.21.0", true), 
                                  "1.0" -> hadoopDependency("1.0.0", false),
                                  "1.0.x" -> hadoopDependency("1.0.0", false),
                                  "cdh" -> hadoopDependency(cdhHadoop, true),
                                  "cdh3" -> hadoopDependency(cdhHadoop, true),
                                  "cloudera" -> hadoopDependency(cdhHadoop, true)
                                 )

  lazy val root = Project( id = "mongo-hadoop", 
                          base = file("."),
                          settings = baseSettings ) aggregate(core, streaming, flume)

  lazy val core = Project( id = "mongo-hadoop-core", 
                           base = file("core"), 
                           settings = coreSettings )

  lazy val streaming = Project( id = "mongo-hadoop-streaming", 
                                base = file("streaming"), 
                                settings = streamingSettings ) dependsOn( core )

  lazy val flume = Project( id = "mongo-flume",
                            base = file("flume"),
                            settings = flumeSettings ) 



  lazy val baseSettings = Defaults.defaultSettings ++ buildSettings ++ Seq( 
    resolvers ++= Seq(Resolvers.mitSimileRepo, Resolvers.clouderaRepo, Resolvers.mavenOrgRepo),
    moduleName <<= (hadoopRelease, moduleName) { (hr, mod) =>
      val rel = coreHadoopMap.getOrElse(hr, 
                    sys.error("Hadoop Release '%s' is an invalid/unsupported release. " +
                              " Valid entries are in %s".format(hr, coreHadoopMap.keySet))
                    )._3

      "%s_%s".format(mod, rel)
    },
    libraryDependencies <<= (libraryDependencies) { deps =>
      
      val scala: ModuleID = deps.find { x => x.name == "scala-library" }.map ( y => 
        y.copy(configurations = Some("test"))
      ).get


      val newDeps = deps.filterNot { x => x.name == "scala-library" }

      newDeps :+ scala
    }

  )

  lazy val parentSettings = baseSettings ++ Seq( 
    publishArtifact := false
  )

  lazy val flumeSettings = baseSettings ++ Seq(
    libraryDependencies ++= Seq(Dependencies.mongoJavaDriver, Dependencies.flume)
  )

  lazy val streamingSettings = baseSettings ++ Seq( 
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
    })
  ) 
      /*
       *(compile in Compile) <<= {inc.Analysis.Empty})
       */
  
  val coreSettings: Seq[sbt.Project.Setting[_]] = baseSettings ++ Seq( 
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



  def hadoopDependency(hadoopVersion: String, useStreaming: Boolean): (Option[() => Seq[ModuleID]], () => Seq[ModuleID], String) = {
      (if (useStreaming) Some(streamingDependency(hadoopVersion)) else None, () => {
      println("*** Adding Hadoop Dependencies for Hadoop '%s'".format(hadoopVersion))

      Seq("org.apache.hadoop" % "hadoop-core" % hadoopVersion)
      }, hadoopVersion)
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
  //val sonatypeSnaps = "snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
  //val sonatypeRels = "releases" at "https://oss.sonatype.org/content/repositories/releases"
  val clouderaRepo = "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
  val mitSimileRepo = "Simile Repo at MIT" at "http://simile.mit.edu/maven"
  val mavenOrgRepo = "Maven.Org Repository" at "http://repo1.maven.org/maven2/org/"
}

object Dependencies {
  val mongoJavaDriver = "org.mongodb" % "mongo-java-driver" % "2.7.3"
  val junit = "junit" % "junit" % "4.10" % "test"
  val flume = "com.cloudera" % "flume-core" % "0.9.4-cdh3u3"
}

// vim: set ts=2 sw=2 sts=2 et:
