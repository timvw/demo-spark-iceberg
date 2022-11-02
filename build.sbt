version := "1.0"
scalaVersion := "2.13.8"

val sparkVersion = "3.3.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-hadoop-cloud" % sparkVersion

libraryDependencies += "org.apache.iceberg" %% "iceberg-spark-runtime-3.3" % "1.0.0"
