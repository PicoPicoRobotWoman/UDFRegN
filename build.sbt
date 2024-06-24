name := "PAPER APACHE SPARK UDF EXAMPLE"
version := "1.0.0"

scalaVersion := "2.12.18"

lazy val scalaVersionStr = "2.12.18"
lazy val sparkVersion = "3.5.0"
lazy val json4sVersion = "3.6.6"

lazy val sparkDependencies = Seq(
	"org.apache.spark" %% "spark-core",
	"org.apache.spark" %% "spark-sql"
).map(_ % sparkVersion % Compile)

lazy val json4sDependencies = Seq(
	"org.scala-lang" % "scala-compiler",
	"org.scala-lang" % "scala-reflect"
).map(_ % scalaVersionStr)

lazy val jsonParserDependencies = Seq(
	"org.json4s" %% "json4s-native",
	"org.json4s" %% "json4s-jackson",
	"org.json4s" %% "json4s-ext"
).map(_ % json4sVersion)

lazy val otherDependencies = Seq(
	"com.lihaoyi" %% "fastparse" % "2.3.0",
	"com.fasterxml.jackson.core" % "jackson-databind" % "2.15.1"
)

libraryDependencies ++= sparkDependencies ++ json4sDependencies ++ jsonParserDependencies ++ otherDependencies
