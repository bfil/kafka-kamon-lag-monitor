name := "kafka-kamon-lag-monitor"
scalaVersion := "2.12.2"
libraryDependencies ++= Seq(
  "io.kamon" %% "kamon-core" % "0.6.6",
  "io.kamon" %% "kamon-influxdb" % "0.6.6",
  "org.apache.kafka" %% "kafka" % "0.10.2.0" exclude("log4j", "log4j") exclude("org.slf4j","slf4j-log4j12"),
  "org.slf4j" % "log4j-over-slf4j" % "1.7.25",
  "ch.qos.logback" % "logback-classic" % "1.2.3"
)