import sbt._
import Keys._
import Reference._
import sbtassembly.Plugin._
import Process._
import AssemblyKeys._

object MongoHadoopBuild extends Build {

  lazy val buildSettings = Seq(
    version := "1.1.0",
    crossScalaVersions := Nil,
    crossPaths := false,
    organization := "org.mongodb"
  )

  /** The version of Hadoop to build against. */
  lazy val hadoopRelease = SettingKey[String]("hadoop-release", "Hadoop Target Release Distro/Version")
  val loadSampleData = TaskKey[Unit]("load-sample-data", "Loads sample data for example programs")


  private val stockPig = "0.9.2"
  private val cdh3Rel = "cdh3u3"
  private val cdh3Hadoop = "0.20.2-%s".format(cdh3Rel) // current "base" version they patch against
  private val cdh3Pig = "0.8.1-%s".format(cdh3Rel)
  private val cdh4Rel = "cdh4.3.0"
  private val cdh4YarnHadoop = "2.0.0-%s".format(cdh4Rel) // current "base" version they patch against
  private val cdh4CoreHadoop = "2.0.0-mr1-%s".format(cdh4Rel)
  private val cdh4Pig = "0.11.0-%s".format(cdh4Rel)

  private val coreHadoopMap = Map("0.20" -> hadoopDependencies("0.20.205.0", false, stockPig),
                                  "0.20.x" -> hadoopDependencies("0.20.205.0", false, stockPig),
                                  "0.21" -> hadoopDependencies("0.21.0", true, stockPig),
                                  "0.21.x" -> hadoopDependencies("0.21.0", true, stockPig),
                                  "0.22" -> hadoopDependencies("0.22.0", true, stockPig, nextGen=true),
                                  "0.22.x" -> hadoopDependencies("0.22.0", true, stockPig, nextGen=true),
                                  "0.23" -> hadoopDependencies("0.23.1", true, stockPig, nextGen=true),
                                  "0.23.x" -> hadoopDependencies("0.23.1", true, stockPig, nextGen=true),
                                  "cdh4" -> hadoopDependencies(cdh4CoreHadoop, true, cdh4Pig, Some(cdh4YarnHadoop), nextGen=true),
                                  "cdh3" -> hadoopDependencies(cdh3Hadoop, true, cdh3Pig),
                                  "1.0" -> hadoopDependencies("1.0.4", false, stockPig),
                                  "1.0.x" -> hadoopDependencies("1.0.4", false, stockPig),
                                  "default" -> hadoopDependencies("1.0.4", false, stockPig)
                                 )

  lazy val root = Project( id = "mongo-hadoop",
                          base = file("."),
                          settings = dependentSettings ++ Seq(loadSampleDataTask)) aggregate(core, flume, pig)

  lazy val core = Project( id = "mongo-hadoop-core",
                           base = file("core"),
                           settings = coreSettings  )

  lazy val hive = Project( id = "mongo-hadoop-hive",
                           base = file("hive"),
                           settings = hiveSettings ) dependsOn( core )


  lazy val pig = Project( id = "mongo-hadoop-pig",
                          base = file("pig"),
                          settings = pigSettings ) dependsOn( core )

  lazy val streaming = Project( id = "mongo-hadoop-streaming",
                                base = file("streaming"),
                                settings = streamingSettings ) dependsOn( core )

  lazy val flume = Project( id = "mongo-flume",
                            base = file("flume"),
                            settings = flumeSettings )

  lazy val cohort = Project( id = "cohort",
                            base = file("examples/cohort"),
                            settings = exampleSettings ) dependsOn( core )

  lazy val treasuryExample  = Project( id = "treasury-example",
                                     base = file("examples/treasury_yield"),
                                     settings = exampleSettings ) dependsOn( core )

  lazy val enronExample  = Project( id = "enron-example",
                                     base = file("examples/enron"),
                                     settings = exampleSettings ) dependsOn( core )

  lazy val sensorsExample  = Project( id = "sensors",
                                     base = file("examples/sensors"),
                                     settings = exampleSettings ) dependsOn( core )

  lazy val baseSettings = Defaults.defaultSettings ++ buildSettings ++ Seq(
    resolvers ++= Seq(Resolvers.mitSimileRepo, Resolvers.clouderaRepo, Resolvers.mavenOrgRepo, Resolvers.sonatypeRels),

    libraryDependencies += ("com.novocode" % "junit-interface" % "0.8" % "test"),
    libraryDependencies += ("commons-lang" % "commons-lang" % "2.3"),
    libraryDependencies <<= (libraryDependencies) { deps =>

      val scala: ModuleID = deps.find { x => x.name == "scala-library" }.map ( y =>
        y.copy(configurations = Some("test"))
      ).get


      val newDeps = deps.filterNot { x => x.name == "scala-library" }

      newDeps :+ scala
    },
    javacOptions ++= Seq("-source", "1.5", "-target", "1.5"),
    javacOptions in doc := Seq("-source", "1.5")
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
    excludedFiles in assembly := { (bases: Seq[File]) => bases flatMap { base =>
      ((base * "*").get collect {
        case f if f.getName.toLowerCase == "git-hash" => f
        case f if f.getName.toLowerCase == "license" => f
      })  ++
      ((base / "META-INF" * "*").get collect {
        case f if f.getName.toLowerCase == "license" => f
        case f if f.getName.toLowerCase == "manifest.mf" => f
      })
    } },
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
    }),
    assembly ~= { (jar) =>
      println("*** Constructed Assembly Jar: %s".format(jar))
      jar
    }

  )

  val exampleSettings = dependentSettings

  val pigSettings = dependentSettings ++ assemblySettings ++ Seq(
    excludedJars in assembly <<= (fullClasspath in assembly) map ( cp =>
      cp filterNot { x =>
        x.data.getName.startsWith("mongo-hadoop-core") || x.data.getName.startsWith("mongo-java-driver") || x.data.getName.startsWith("mongo-hadoop-pig")
      }
    ),
    excludedFiles in assembly := { (bases: Seq[File]) => bases flatMap { base =>
      ((base * "*").get collect {
        case f if f.getName.toLowerCase == "git-hash" => f
        case f if f.getName.toLowerCase == "license" => f
      })  ++
      ((base / "META-INF" * "*").get collect {
        case f if f.getName.toLowerCase == "license" => f
        case f if f.getName.toLowerCase == "manifest.mf" => f
      })
    } },

    resolvers ++= Seq(Resolvers.rawsonApache), /** Seems to have thrift deps I need*/
    libraryDependencies <++= (scalaVersion, libraryDependencies, hadoopRelease) { (sv, deps, hr: String) =>

      val hadoopDeps = coreHadoopMap.getOrElse(hr, sys.error("Hadoop Release '%s' is an invalid/unsupported release. Valid entries are in %s".format(hr, coreHadoopMap.keySet)))
      hadoopDeps._4()
    }
  )

  val hiveSettings = dependentSettings ++ assemblySettings ++ Seq(
    excludedJars in assembly <<= (fullClasspath in assembly) map ( cp =>
      cp filterNot { x =>
        x.data.getName.startsWith("mongo-hadoop-core") || x.data.getName.startsWith("mongo-java-driver") || x.data.getName.startsWith("mongo-hadoop-hive")
      }
    ),
    excludedFiles in assembly := { (bases: Seq[File]) => bases flatMap { base =>
      ((base * "*").get collect {
        case f if f.getName.toLowerCase == "git-hash" => f
        case f if f.getName.toLowerCase == "license" => f
      })  ++
      ((base / "META-INF" * "*").get collect {
        case f if f.getName.toLowerCase == "license" => f
        case f if f.getName.toLowerCase == "manifest.mf" => f
      })
    } },

    resolvers ++= Seq(Resolvers.rawsonApache), /** Seems to have thrift deps I need*/
    libraryDependencies ++= Seq(Dependencies.hiveSerDe)
  )

  val coreSettings = dependentSettings ++ Seq(
    libraryDependencies ++= Seq(Dependencies.mongoJavaDriver, Dependencies.junit),
    libraryDependencies <++= (scalaVersion, libraryDependencies, hadoopRelease) { (sv, deps, hr: String) =>

      val hadoopDeps = coreHadoopMap.getOrElse(hr, sys.error("Hadoop Release '%s' is an invalid/unsupported release. Valid entries are in %s".format(hr, coreHadoopMap.keySet)))
      hadoopDeps._2()
    },
    libraryDependencies <<= (scalaVersion, libraryDependencies) { (sv, deps) =>
      val versionMap = Map("2.8.1" -> ("specs2_2.8.1", "1.5"),
                           "2.9.0" -> ("specs2_2.9.0", "1.7.1"),
                           "2.9.0-1" -> ("specs2_2.9.0", "1.7.1"),
                           "2.9.1" -> ("specs2_2.9.1", "1.7.1"),
                           "2.9.2" -> ("specs2_2.9.2", "1.10"))
      val tuple = versionMap.getOrElse(sv, sys.error("Unsupported Scala version '%s' for Specs2".format(sv)))
      deps :+ ("org.specs2" % tuple._1 % tuple._2 % "test")
    },
    autoCompilerPlugins := true,
    parallelExecution in Test := true,
    testFrameworks += TestFrameworks.Specs2
  )

  val loadSampleDataTask = loadSampleData := {
    println("Loading Sample data ...")
    "mongoimport -d mongo_hadoop -c yield_historical.in --drop examples/treasury_yield/src/main/resources/yield_historical_in.json" !

    "mongoimport -d mongo_hadoop -c ufo_sightings.in --drop examples/ufo_sightings/src/main/resources/ufo_awesome.json"  !

    println("********")
    println("*** Loading the Enron, Word Count and Twitter sample data requires manual intervention.  Please see the respective documentation")
    println("********")
  }


  def hadoopDependencies(hadoopVersion: String, useStreaming: Boolean, pigVersion: String, altStreamingVer: Option[String] = None, nextGen: Boolean = false): (Option[() => Seq[ModuleID]], () => Seq[ModuleID], String, () => Seq[ModuleID]) = {
      (if (useStreaming) Some(streamingDependency(altStreamingVer.getOrElse(hadoopVersion))) else None, () => {
      println("*** Adding Hadoop Dependencies for Hadoop '%s'".format(altStreamingVer.getOrElse(hadoopVersion)))

      val deps = if (altStreamingVer.isDefined || nextGen)
        Seq("org.apache.hadoop" % "hadoop-common" % altStreamingVer.getOrElse(hadoopVersion))
      else
        Seq("org.apache.hadoop" % "hadoop-core" % hadoopVersion/*,
            ("org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion notTransitive()).exclude("commons-daemon", "commons-daemon")*/)
      if (nextGen) {

        def mrDep(mod: String) = "org.apache.hadoop" % "hadoop-mapreduce-client-%s".format(mod) % altStreamingVer.getOrElse(hadoopVersion) notTransitive() exclude("org.apache.hadoop", "hdfs")


        if (hadoopVersion.startsWith("0.22")) {
            deps ++ Seq("org.apache.hadoop" % "hadoop-mapred" % altStreamingVer.getOrElse(hadoopVersion))
        } else if (hadoopVersion.startsWith("0.23") || hadoopVersion.startsWith("2.0.0")) {
          deps ++ Seq(mrDep("core"), mrDep("common"), mrDep("shuffle"),
                      mrDep("shuffle"), mrDep("app"), mrDep("jobclient"))
        } else {
          sys.error("Don't know how to do any special mapping for Hadoop Version " + hadoopVersion)
        }
      }
      else
        deps


      }, hadoopVersion, pigDependency(pigVersion))
  }

  def pigDependency(pigVersion: String): () => Seq[ModuleID] = {
    () => {
      println("*** Adding Pig Dependency for Version '%s'".format(pigVersion))

      Seq(
        "org.apache.pig" % "pig" % pigVersion,
        "org.antlr" % "antlr" % "3.4"
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
  val mavenOrgRepo = "Maven.Org Repository" at "http://repo1.maven.org/maven2/"
  /** Seems to have thrift deps I need*/
  val rawsonApache = "rawsonApache" at "http://people.apache.org/~rawson/repo/"
  val nictaScoobi = "Nicta Scoobi" at "http://nicta.github.com/scoobi/releases/"

}

object Dependencies {
  val mongoJavaDriver = "org.mongodb" % "mongo-java-driver" % "2.10.1"
  val junit = "junit" % "junit" % "4.10" % "test"
  val flume = "com.cloudera" % "flume-core" % "0.9.4-cdh3u3"
  val hiveSerDe = "org.apache.hive" % "hive-serde" % "0.9.0"
  val casbah = "org.mongodb" %% "casbah" % "2.3.0"
  val scoobi = "com.nicta" %% "scoobi" % "0.4.0" % "provided"
}

// vim: set ts=2 sw=2 sts=2 et:
