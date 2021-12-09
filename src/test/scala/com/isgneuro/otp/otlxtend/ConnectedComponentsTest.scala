package com.isgneuro.otp.otlxtend

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

class ConnectedComponentsTest extends CommandTest{
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
      (1, 0),
      (1, 2),
      (2, 5),
      (3, 4),
      (4, 6)
    ).toDF("src", "dst")

    val expected = Seq(
      (1, 0, 0),
      (1, 2, 0),
      (2, 5, 0),
      (3, 4, 3),
      (4, 6, 3)
    ).toDF("src", "dst", "cc")

    val query = ""
    val actual = new ConnectedComponents(SimpleQuery(query), utils).transform(dataset)
    assert(actual.except(expected).count() == 0)
  }

  test("Use specified names for source and destination") {
    val dataset = Seq(
      (1, 0),
      (1, 2),
      (2, 5),
      (3, 4),
      (4, 6)
    ).toDF("node", "relation")

    val expected = Seq(
      (1, 0, 0),
      (1, 2, 0),
      (2, 5, 0),
      (3, 4, 3),
      (4, 6, 3)
    ).toDF("node", "relation", "cc")

    val query = "src=node dst=relation"
    val actual = new ConnectedComponents(SimpleQuery(query), utils).transform(dataset)
    assert(actual.except(expected).count() == 0)
  }
}
