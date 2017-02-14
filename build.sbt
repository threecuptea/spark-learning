
name := "spark-learning"

version := "1.0"

scalaVersion := "2.10.6"

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.3" % "provided",
  "hadoop-lzo" % "hadoop-lzo" % "0.4.15-gplextras5.0.0",
  "com.hadoop.gplcompression" % "hadoop-lzo" % "0.4.19"
)




    