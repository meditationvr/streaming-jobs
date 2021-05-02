name := "streaming-job"

version := "0.1"
scalaVersion := "2.11.8"
val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion  % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion  % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion  % "provided",
  "com.datastax.spark" %% "spark-cassandra-connector" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" %  sparkVersion  % "provided",
  "io.spray" %%  "spray-json" % "1.3.5"
)

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.first
  case x => (assemblyMergeStrategy in assembly).value(x)
}