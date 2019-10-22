name := "segmentation"

version := "1.0"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.2" //% Provided

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2" // % Provided

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test

libraryDependencies += "org.mockito" % "mockito-all" % "1.10.19" % Test