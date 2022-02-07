name := "OTLExtend"

version := "0.4.5"

scalaVersion := "2.11.12"

resolvers += Resolver.jcenterRepo

libraryDependencies += "ot.dispatcher" % "dispatcher-sdk_2.11" % "1.2.0"  % Compile
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.3" % Compile

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