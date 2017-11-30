name := "spark-learn"

version := "0.1"

scalaVersion := "2.11.2"

libraryDependencies += "org.apache.spark" %%"spark-core" % "2.2.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % Test
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.0" % "provided"


        