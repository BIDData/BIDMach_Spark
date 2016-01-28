
name := "BIDMatHDFS"

version := "1.0.3"

organization := "edu.berkeley.bid"

scalaVersion := "2.11.2"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  "../../BIDMatHDFS.jar"
}

resolvers ++= Seq(
  "Scala Tools Snapshots" at "http://scala-tools.org/repo-snapshots/",
  "Scala Mirror" at "https://oss.sonatype.org/content/repositories/releases/"
)

libraryDependencies <<= (scalaVersion, libraryDependencies) { (sv, deps) =>
  deps :+ ("org.scala-lang" % "scala-compiler" % sv)
}

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

scalacOptions ++= Seq("-feature","-deprecation","-target:jvm-1.7")

javaOptions += "-Xmx12g"

