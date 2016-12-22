name := "FlinkStudy"
version := "1.0"
scalaVersion := "2.11.8"

mainClass in assembly := Some("flink.sample.SampleApp")
assemblyJarName := "flink.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies += "org.apache.flink" % "flink-streaming-scala_2.11" % "1.1.3"
libraryDependencies += "org.apache.flink" % "flink-table_2.11" % "1.1.3"
libraryDependencies += "org.apache.flink" % "flink-scala_2.11" % "1.1.3"
libraryDependencies += "org.apache.flink" % "flink-clients_2.11" % "1.1.3"
libraryDependencies += "org.apache.flink" % "flink-connector-kafka-0.9_2.11" % "1.1.3"
libraryDependencies += "org.apache.flink" % "flink-cep-scala_2.11" % "1.1.3"