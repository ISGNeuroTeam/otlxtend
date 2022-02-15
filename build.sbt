name := "otlxtend"

version := "0.5.1"

scalaVersion := "2.12.10"

resolvers += Resolver.jcenterRepo

libraryDependencies += "ot.dispatcher" % "dispatcher-sdk_2.12" % "2.0.0"  % Compile
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.1.2" % Compile

credentials += Credentials(
  "Sonatype Nexus Repository Manager",
  sys.env.getOrElse("NEXUS_HOSTNAME", ""),
  sys.env.getOrElse("NEXUS_COMMON_CREDS_USR", ""),
  sys.env.getOrElse("NEXUS_COMMON_CREDS_PSW", "")
)

publishTo := Some(
  "Sonatype Nexus Repository Manager" at sys.env.getOrElse("NEXUS_OTP_URL_HTTPS", "")
    + "/repository/ot.platform-sbt-releases"
)