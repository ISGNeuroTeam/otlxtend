package com.isgneuro.otp.otlxtend

import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{DataFrame, SparkSession}
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

class OTLLatestRowTest extends CommandTest {
  private lazy val sparkSession: SparkSession =
    SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()
  import sparkSession.implicits._

  val dataset = """[{}]"""

  private def show(dfs: DataFrame*): Unit = {
    dfs.foreach(_.show())
  }

  test("Get latest row with default timeCol and no groupBy") {
    val source = sparkSession.range(10).toDF("id")
      .withColumn("mod2", expr("""id % 2"""))
      .withColumn("mod3", expr("""id % 3"""))
      .withColumnRenamed("id", "_time")
    val expected = Seq((9, 1, 0)).toDF("_time", "mod2", "mod3")
    val query = ""
    val actual = new OTLLatestRow(SimpleQuery(query), utils).transform(source)
    assert(actual.except(expected).count() == 0)
  }

  test("Get latest row with by statement") {
    val source = sparkSession.range(10).toDF("id")
      .withColumn("mod2", expr("""id % 2"""))
      .withColumn("mod3", expr("""id % 3"""))
      .withColumnRenamed("id", "_time")
    val expected = Seq(
      (8, 0, 2),
      (9, 1, 0)).toDF("_time", "mod2", "mod3")
    val query = "by mod2"
    val actual = new OTLLatestRow(SimpleQuery(query), utils).transform(source)
    assert(actual.except(expected).count() == 0)
  }

  test("Get latest row with specified timeCol") {
    val source = sparkSession.range(10).toDF("id")
      .withColumn("mod2", expr("""id % 2"""))
      .withColumn("mod3", expr("""id % 3"""))
    val expected = Seq(
      (9, 1, 0)).toDF("_time", "mod2", "mod3")
    val query = "_time=id"
    val actual = new OTLLatestRow(SimpleQuery(query), utils).transform(source)
    assert(actual.except(expected).count() == 0)
  }

  test("Get latest row with window engine") {
    val source = sparkSession.range(10).toDF("id")
      .withColumn("mod2", expr("""id % 2"""))
      .withColumn("mod3", expr("""id % 3"""))
    val expected = Seq(
      (9, 1, 0)).toDF("_time", "mod2", "mod3")
    val query = "_time=id engine=window"
    val actual = new OTLLatestRow(SimpleQuery(query), utils).transform(source)
    assert(actual.except(expected).count() == 0)
  }

  test("Get latest row with by statement and window engine") {
    val source = sparkSession.range(10).toDF("id")
      .withColumn("mod2", expr("""id % 2"""))
      .withColumn("mod3", expr("""id % 3"""))
      .withColumnRenamed("id", "_time")
    val expected = Seq(
      (8, 0, 2),
      (9, 1, 0)).toDF("_time", "mod2", "mod3")
    val query = "engine=window by mod2"
    val actual = new OTLLatestRow(SimpleQuery(query), utils).transform(source)
    assert(actual.except(expected).count() == 0)
  }
}
