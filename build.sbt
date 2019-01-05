name := "Scintilla"

version := "1.0"

scalaVersion := "2.12.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

coverageExcludedPackages := ".*Playground.*"
