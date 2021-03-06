name := "Spark Twitter"

version := "1.0"

scalaVersion := "2.11.8"

// 2.0.1 is the latest
val sparkVersion = "2.1.0"
val configVersion = "1.3.1"

libraryDependencies ++= Seq(
 "org.apache.spark" %% "spark-core" % sparkVersion,
 "org.apache.spark" %% "spark-sql" % sparkVersion,
 "org.apache.spark" %% "spark-streaming" % sparkVersion,
 "org.apache.bahir" %% "spark-streaming-twitter" % sparkVersion,
 "com.typesafe" % "config" % configVersion
)

fork in run := true