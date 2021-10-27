package com.isgneuro.otp.otlxtend

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

class OTLPivotTest extends CommandTest {
  private lazy val sparkSession: SparkSession =
    SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

  import sparkSession.implicits._

  val dataset = """[{}]"""

  test("OTLPivot should pivot table with multiple columns fixed") {
    val input = Seq((1, "a", "m1", 10, 1),
      (1, "a", "m2", 11, 2),
      (1, "a", "m3", 12, 2),
      (1, "b", "m1", 20, 2),
      (1, "b", "m2", 21, 1),
      (1, "b", "m3", 22, 4),
      (2, "a", "m1", 100, 9),
      (2, "a", "m2", 101, 3),
      (2, "a", "m3", 102, 2),
      (2, "b", "m1", 200, 5),
      (2, "b", "m2", 201, 5),
      (2, "b", "m3", 202, 2)
    ).toDF("time", "well", "metric", "value", "useless")
    val expected = Seq((1, "a", 10, 11, 12),
      (1, "b", 20, 21, 22),
      (2, "a", 100, 101, 102),
      (2, "b", 200, 201, 202)).toDF("time", "well", "m1", "m2", "m3")
      .orderBy(col("time"), col("well"))
      .select("time", "well", "m1", "m2", "m3")
    val query = "time, well, metric, value"
    val actual = new OTLPivot(SimpleQuery(query), utils).transform(input)
      .orderBy(col("time"), col("well"))
      .select("time", "well", "m1", "m2", "m3")
    assert(actual.except(expected).count() === 0)
  }

  test("OTLPivot should add fake column if only two columns specified") {
    val input = Seq((1, "a", "m1", 10, 1),
      (1, "a", "m2", 11, 2),
      (1, "a", "m3", 12, 2),
      (1, "b", "m1", 20, 2),
      (1, "b", "m2", 21, 1),
      (1, "b", "m3", 22, 4),
      (2, "a", "m1", 100, 9),
      (2, "a", "m2", 101, 3),
      (2, "a", "m3", 102, 2),
      (2, "b", "m1", 200, 5),
      (2, "b", "m2", 201, 5),
      (2, "b", "m3", 202, 2)
    ).toDF("time", "well", "metric", "value", "useless")
    val expected = Seq((10, 11, 12)).toDF("m1", "m2", "m3")
      .select("m1", "m2", "m3")
    val query = "metric, value"
    val actual = new OTLPivot(SimpleQuery(query), utils).transform(input)
      .select("m1", "m2", "m3")
    assert(actual.except(expected).count() === 0)
  }
}