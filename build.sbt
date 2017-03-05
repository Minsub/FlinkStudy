name := "FlinkStudy"
version := "1.0"
scalaVersion := "2.11.8"

mainClass in assembly := Some("flink.sample.SampleApp")
assemblyJarName := "flink.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies += "org.apache.flink" % "flink-streaming-scala_2.11" % "1.2.0"
libraryDependencies += "org.apache.flink" % "flink-table_2.11" % "1.2.0"
libraryDependencies += "org.apache.flink" % "flink-scala_2.11" % "1.2.0"
libraryDependencies += "org.apache.flink" % "flink-cep-scala_2.11" % "1.2.0"
libraryDependencies += "org.apache.flink" % "flink-clients_2.11" % "1.2.0"
libraryDependencies += "org.apache.flink" % "flink-connector-kafka-0.10_2.11" % "1.2.0"
libraryDependencies += "org.apache.flink" % "flink-connector-elasticsearch2_2.11" % "1.2.0"
libraryDependencies += "org.apache.flink" % "flink-connector-twitter_2.11" % "1.2.0"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.4"
libraryDependencies += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.4"
libraryDependencies += "org.scalikejdbc" % "scalikejdbc_2.11" % "2.5.0"
libraryDependencies += "org.apache.commons" % "commons-email" % "1.4"


