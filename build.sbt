name := "dw_unit_tests"

version := "0.1"

scalaVersion := "2.12.7"

mainClass in (Compile, run) := Some("com.angelini.dw_unit_tests.Main")

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)

libraryDependencies ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.7",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.7",
  "com.google.cloud" % "google-cloud-storage" % "1.48.0",
  "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
)
