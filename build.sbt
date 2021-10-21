lazy val root = (project in file(".")).
  settings(
    name := "Scale-Spark",
    version := "1.0",
    scalaVersion := "2.13",
    mainClass in Compile := Some("com.myscala.spark.DataFrameFactorer")
  )

val sparkVersion = "3.1.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core_2.12" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming_2.12" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql_2.12" % sparkVersion % "provided"
)

// META-INF discarding
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
}