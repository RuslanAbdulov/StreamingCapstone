name := "SparkStreaming"

version := "0.1"

scalaVersion := "2.11.12"

sparkVersion := "2.4.0"


libraryDependencies += "org.apache.spark" % "spark-core_2.11" % sparkVersion.value
libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % sparkVersion.value
//libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.0" % "compile"

libraryDependencies += "org.apache.ignite" % "ignite-spark" % "2.6.0"

//libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.2.0"
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.1.0"


libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % sparkVersion.value

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % sparkVersion.value
libraryDependencies += "org.apache.spark" % "spark-catalyst_2.11" % sparkVersion.value



assemblyJarName in assembly := s"${name.value.replace(' ','-')}-${version.value}.jar"
