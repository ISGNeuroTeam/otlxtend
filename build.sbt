name := "otlxtend"

version := "0.5.1"

scalaVersion := "2.11.12"

resolvers += Resolver.jcenterRepo

resolvers += ("Sonatype OSS Snapshots" at (sys.env.getOrElse("NEXUS_OTP_URL_HTTPS","http://storage.dev.isgneuro.com")
  + "/repository/ot.platform-sbt-releases/")).withAllowInsecureProtocol(true)

libraryDependencies += "ot.dispatcher" % "dispatcher-sdk_2.11" % "1.2.2" % Compile

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.8" % Compile

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