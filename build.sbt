name := s"${FttBuild.namePrefix}Root"


val projectVersion = "0.1.0-SNAPSHOT"
val sparkVersion = "2.3.0"

/*
    Common settingssbt tasks
 */
lazy val baseSettings = Seq(
  organization := "com.saadlamarti",
  version := projectVersion,
  scalaVersion := "2.11.12",
  resolvers ++= Seq(
    Opts.resolver.sonatypeReleases
  ),
  dependencyOverrides ++= Seq(
    // jackson is conflict among Google's libraries and Spark's libraries
    // TODO: validate versions
    "com.fasterxml.jackson.core" % "jackson-core" % "2.6.2",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.2"
  ),
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.0.0" % Test
  ),
  fork in Test := true,
  logBuffered in Test := false,
  parallelExecution in Test := false
)

lazy val commonSettings = Seq(
  libraryDependencies ++= Seq(
    "commons-cli" % "commons-cli" % "1.4"
  )
)

/*
    Spark
 */


lazy val sparkDependencies: Seq[ModuleID] =  Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion
).map(_  % Provided)

/*
  Test
 */
lazy val testDependencies: Seq[ModuleID] =  Seq(
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_0.10.0",
  "org.scalatest" %% "scalatest" % "3.0.5",
  "org.apache.spark" %% "spark-mllib" % sparkVersion
).map(_ % "it,test")

conflictManager := ConflictManager.strict

dependencyOverrides += "org.scalatest" %% "scalatest" % "3.0.5"

lazy val googleDependencies: Seq[ModuleID] = Seq(
  "com.google.cloud" % "google-cloud-bigquery" % "1.32.0"
)

/*
    Assembly
 */
lazy val assemblySettings = Seq(
  test in assembly := {},
  assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
  // See http://queirozf.com/entries/creating-scala-fat-jars-for-spark-on-sbt-with-sbt-assembly-plugin#spark2
)

/*
    Projects definition
 */
lazy val common = (project in file("common"))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings: _*)
  .settings(baseSettings ++ commonSettings ++ Seq(
    libraryDependencies ++= sparkDependencies,
    libraryDependencies ++= testDependencies,
    libraryDependencies ++= googleDependencies))
  .disablePlugins(sbtassembly.AssemblyPlugin)

lazy val root = (project in file("."))
  .settings(baseSettings)
  .aggregate(common)
  .disablePlugins(sbtassembly.AssemblyPlugin) // Ignore on `assembly` cmd
