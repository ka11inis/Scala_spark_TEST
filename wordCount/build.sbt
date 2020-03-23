name := "erg4Ypoer1"

version := "1.0"

scalaVersion := "2.10.6"

val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion//,
  //"org.apache.spark" %% "spark-sql" % sparkVersion //,
  //"org.apache.spark" %% "spark-mllib" % sparkVersion
  //"org.apache.spark" %% "spark-streaming" % sparkVersion,
  //"org.apache.spark" %% "spark-hive" % sparkVersion
)
