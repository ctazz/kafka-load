import com.typesafe.sbt.SbtNativePackager
import com.typesafe.sbt.packager.universal.UniversalPlugin.autoImport._

name := "kafka-load"

organization := "org.fjord.morgue"

releaseSettings

scalaVersion := "2.10.6"

lazy val root = (project in file("."))
  .enablePlugins(SbtNativePackager, JavaAppPackaging, UniversalPlugin)

bashScriptConfigLocation := Some("${app_home}/../conf/jvmopts")

bashScriptExtraDefines += """addJava "-Dconfig.file=${app_home}/../conf/application.conf""""

bashScriptExtraDefines += """addJava "-Dlogback.configurationFile=${app_home}/../conf/logback.xml""""

bashScriptExtraDefines += """addJava "-Dlog.dir=${log_dir}""""


libraryDependencies ++= Seq(
  "com.101tec" % "zkclient" % "0.3"
  , "org.apache.zookeeper" % "zookeeper" % "3.4.6"
  ,"org.apache.kafka" % "kafka_2.10" % "0.8.2.1" exclude("com.sun.jdmk", "jmxtools") exclude("com.sun.jmx", "jmxri") exclude("org.slf4j", "slf4j-log4j12")
  , "org.apache.kafka" % "kafka-clients" % "0.8.2.1"
  , "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"
  , "com.fasterxml.jackson.datatype" % "jackson-datatype-joda" % "2.4.4"
  , "com.fasterxml.jackson.module" % "jackson-module-scala_2.10" % "2.4.4"
  , "com.typesafe.akka" % "akka-actor_2.10" % "2.3.8"
  , "com.typesafe" % "config" % "1.2.0"
)



libraryDependencies ++= Seq(
  "junit" % "junit" % "4.12" % Test,
  "org.specs2" %% "specs2" % "2.4.16" % Test,
  "com.typesafe.akka" %% "akka-testkit" % "2.3.9" % Test
)

autoCompilerPlugins := true

resolvers ++= Seq(
  "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases",
  "Scala-tools" at "https://oss.sonatype.org/content/repositories/snapshots"
)

val packageZip = taskKey[File]("package-zip")

packageZip := (baseDirectory in Compile).value / "target" / "universal" / (name.value + "-" + version.value + ".zip")

artifact in (Universal, packageZip) ~= { (art:Artifact) => art.copy(`type` = "zip", extension = "zip") }

addArtifact(artifact in (Universal, packageZip), packageZip in Universal)

publish <<= (publish) dependsOn (packageBin in Universal)

Keys.mainClass in (Compile) := Some("tools.Driver")

publishMavenStyle := true

parallelExecution in Test := false
