package com.isgneuro.otp.otlxtend

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import ot.dispatcher.sdk.test.CommandTest

class ConnectedComponents extends CommandTest{
  override val dataset: String = ""

  private val logger = Logger.getLogger("org.apache")
  logger.setLevel(Level.FATAL)

  private lazy val sparkSession: SparkSession =
    SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()
  import sparkSession.implicits._

  test("Identify connected components") {
    val dataset = Seq(
      ()
    )
  }
}
