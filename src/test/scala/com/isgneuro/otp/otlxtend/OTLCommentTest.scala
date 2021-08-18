package com.isgneuro.otp.otlxtend

import org.apache.spark.sql.{ DataFrame, SparkSession }
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest
import org.apache.log4j.{ Logger, Level }

class OTLCommentTest extends CommandTest{
  private val logger = Logger.getLogger("org.apache")
  logger.setLevel(Level.FATAL)

  private lazy val sparkSession: SparkSession =
    SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()
  import sparkSession.implicits._

  val dataset = """[{}]"""

  test("Test 0. OTLComment | Keep the same dataframe with no any changes") {
    val source = Seq(
      (1, 2, "3"),
      (2, 3, "4"),
      (5, 6, "7")
    ).toDF("x", "y", "name")
    val expected = Seq(
      (1, 2, "3"),
      (2, 3, "4"),
      (5, 6, "7")
    ).toDF("x", "y", "name")
    val query = """ "comment with a lot of words """
    val actual = new OTLComment(SimpleQuery(query), utils).transform(source)
    assert(expected.columns.sorted sameElements Array("name", "x", "y"))
    assert(actual.except(expected).count() == 0)
  }
}
