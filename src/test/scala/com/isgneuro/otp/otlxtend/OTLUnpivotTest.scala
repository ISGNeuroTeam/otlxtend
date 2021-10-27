package com.isgneuro.otp.otlxtend

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

class OTLUnpivotTest extends CommandTest {
  private lazy val sparkSession: SparkSession =
    SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

  import sparkSession.implicits._

  val dataset = """[{}]"""

  test("OTLUnpivot should unpivot table with multiple columns fixed") {
    val expected = Seq((1, "a", "m1", 10),
      (1, "a", "m2", 11),
      (1, "a", "m3", 12),
      (1, "b", "m1", 20),
      (1, "b", "m2", 21),
      (1, "b", "m3", 22),
      (2, "a", "m1", 100),
      (2, "a", "m2", 101),
      (2, "a", "m3", 102),
      (2, "b", "m1", 200),
      (2, "b", "m2", 201),
      (2, "b", "m3", 202)
    ).toDF("time", "well", "metric", "value")
    val input = Seq((1, "a", 10, 11, 12),
      (1, "b", 20, 21, 22),
      (2, "a", 100, 101, 102),
      (2, "b", 200, 201, 202)).toDF("time", "well", "m1", "m2", "m3")
      .orderBy(col("time"), col("well"))
      .select("time", "well", "m1", "m2", "m3")
    val query = "time, well, metric, value"
    val actual = new OTLUnpivot(SimpleQuery(query), utils).transform(input)
      .orderBy(col("time"), col("well"))
      .select("time", "well", "metric", "value")
    assert(actual.except(expected).count() === 0)
  }
}