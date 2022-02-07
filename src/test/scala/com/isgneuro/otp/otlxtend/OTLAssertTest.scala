package com.isgneuro.otp.otlxtend

import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

class OTLAssertTest extends CommandTest {
  import spark.implicits._

  override val dataset: String = """[{}]"""

  test("Return intact dataframe if assertion is true") {
    val sourceDf: DataFrame =
      Seq(
        (1, 2, "3"),
        (2, 3, "4"),
        (5, 6, "7")
      ).toDF("x", "y", "name")

    val expectedDf: DataFrame =
      Seq(
        (1, 2, "3"),
        (2, 3, "4"),
        (5, 6, "7")
      ).toDF("x", "y", "name")

    val query: String = "x is not null"
    val actualDf: DataFrame = new OTLAssert(SimpleQuery(query), utils).transform(sourceDf)

    assert(expectedDf.columns.sorted sameElements Array("name", "x", "y"))
    assert(actualDf.except(expectedDf).count() == 0)
  }

  test("An exception should be thrown if the assertion is false") {
    val sourceDf: DataFrame =
      Seq(
        (1, 2, "3"),
        (2, 3, "4"),
        (5, 6, "7")
      ).toDF("x", "y", "name")

    val query: String = "x is null"

    val exception: Exception = intercept[Exception] {
      new OTLAssert(SimpleQuery(query), utils).transform(sourceDf)
    }

    assert(exception.getMessage.startsWith("Assertion rule") && exception.getMessage.endsWith("failed"))
  }

}
