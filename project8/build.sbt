name := "Project8"
version := "0.1"

scalaVersion := "2.11.8"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"
libraryDependencies += "net.ruippeixotog" %% "scala-scraper" % "2.1.0"


val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"        % sparkVersion,
  "org.apache.spark" %% "spark-streaming"   % sparkVersion,
  "org.apache.spark" %% "spark-mllib"       % sparkVersion,
  "org.apache.spark" %% "spark-graphx"      % sparkVersion,
  "org.apache.spark" %% "spark-sql"         % sparkVersion
)