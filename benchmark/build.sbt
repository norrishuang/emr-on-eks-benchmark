name := "eks-spark-benchmark"

version := "1.0"

scalaVersion := "2.12.10"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

Compile / unmanagedBase := baseDirectory.value / "libs"

// Dependencies required for this project
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.2.0" % "provided",
  // JSON serialization
  "org.json4s" %% "json4s-native" % "3.6.7",
  // scala logging
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"
)

// Remove stub classes
assembly / assemblyMergeStrategy := {
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.discard
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}

// Exclude the Scala runtime jars
assembly / assemblyPackageScala / assembleArtifact := false

resolvers ++= Seq(
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  Resolver.sonatypeRepo("public")
)
