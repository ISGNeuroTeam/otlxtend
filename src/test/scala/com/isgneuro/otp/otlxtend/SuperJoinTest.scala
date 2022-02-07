package com.isgneuro.otp.otlxtend

import org.apache.spark.sql.{DataFrame, SaveMode}
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

class SuperJoinTest extends CommandTest {
  import spark.implicits._

  override val dataset: String = """[{}]"""

  test("Inner join with dataframe stored in parquet on field=name") {
    val sourceDf: DataFrame =
      Seq(
        (1, "3"),
        (2, "4"),
        (5, "7")
      ).toDF("x", "name")

    val expectedDf: DataFrame =
      Seq(
        ("3", 1, 2),
        ("4", 2, 3),
        ("7", 5, 6)
      ).toDF("x", "y", "name")

    val query: String = "type=inner path=src/test/resources/superJoin format=parquet name"
    val actualDf: DataFrame = new SuperJoin(SimpleQuery(query), utils).transform(sourceDf)

    assert(expectedDf.columns.sorted sameElements Array("name", "x", "y"))
    assert(actualDf.except(expectedDf).count() == 0)
  }
}
