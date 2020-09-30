name := "SparkIngestion"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq( "org.apache.spark" % "spark-core_2.11" % "2.1.0","org.apache.spark" %% "spark-sql" % "2.1.0","mysql" % "mysql-connector-java" % "5.1.12")