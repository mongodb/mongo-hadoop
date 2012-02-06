import sbt._
import Keys._
import Reference._

object MongoHadoopBuild extends Build {

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
                          aggregate = subProjects,
                          settings = baseSettings )

  lazy val core = Project( id = "mongo-hadoop-core", 
                           base = file("core"), 
                           settings = coreSettings )

  lazy val streaming = Project( id = "mongo-hadoop-streaming", 
                                base = file("streaming"), 
                                settings = streamingSettings ) dependsOn( core )

  lazy val flume = Project( id = "mongo-flume",
                            base = file("flume"),
                            settings = flumeSettings ) 

  private lazy val subProjects: Seq[ProjectReference] = scala.collection.mutable.Seq(projectToRef(core),
                                                                                     projectToRef(flume))


  lazy val baseSettings = Defaults.defaultSettings ++ Seq( 
    hadoopRelease := "1.0", 
    resolvers ++= Seq(Resolvers.clouderaRepo, Resolvers.mavenOrgRepo)
  )

  lazy val parentSettings = baseSettings ++ Seq( 
    publishArtifact := false
  )

  lazy val flumeSettings = baseSettings ++ Seq(
    libraryDependencies ++= Seq("com.cloudera" % "flume-core" % "0.9.4-cdh3u3")
  )

  lazy val streamingSettings = baseSettings ++ Seq( 
    libraryDependencies <++= (scalaVersion, libraryDependencies, hadoopRelease) { (sv, deps, hr: String) => 

      val streamingDeps = coreHadoopMap.getOrElse(hr, sys.error("Hadoop Release '%s' is an invalid/unsupported release. Valid entries are in %s".format(hr, coreHadoopMap.keySet)))
      streamingDeps._1.getOrElse(() => Seq.empty[ModuleID])()
  })
  
  lazy val coreSettings: Seq[sbt.Project.Setting[_]] = baseSettings ++ Seq( 
    libraryDependencies ++= Seq("org.mongodb" % "mongo-java-driver" % "2.7.3"),
    libraryDependencies <++= (scalaVersion, libraryDependencies, hadoopRelease) { (sv, deps, hr: String) => 

      val hadoopDeps = coreHadoopMap.getOrElse(hr, sys.error("Hadoop Release '%s' is an invalid/unsupported release. Valid entries are in %s".format(hr, coreHadoopMap.keySet)))
      if (hadoopDeps._1.isDefined) { subProjects :+ projectToRef(streaming) }
      hadoopDeps._2()
  })



def hadoopDependency(hadoopVersion: String, useStreaming: Boolean): (Option[() => Seq[ModuleID]], () => Seq[ModuleID]) = {
    (if (useStreaming) Some(streamingDependency(hadoopVersion)) else None, () => {
      println("*** Adding Hadoop Dependencies for Hadoop '%s'".format(hadoopVersion))

      Seq("org.apache.hadoop" % "hadoop-core" % hadoopVersion)
    })
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
  val clouderaRepo = "Cloudera Repository" at "https://repository.cloudera.com/content/repositories/releases/"
  val mavenOrgRepo = "Maven.Org Repository" at "http://repo1.maven.org/maven2/org/"
}

// vim: set ts=2 sw=2 sts=2 et:
