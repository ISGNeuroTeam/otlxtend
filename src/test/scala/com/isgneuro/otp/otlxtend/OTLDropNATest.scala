package com.isgneuro.otp.otlxtend

import org.apache.spark.sql.SparkSession
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

class OTLDropNATest extends CommandTest{
  private lazy val sparkSession: SparkSession =
    SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()
  import sparkSession.implicits._

  val dataset = """[{}]"""

  test("Drop rows with missing values in columns specified in subset keyword") {
    val source = Seq(
      (Some(1), Some(2), 3),
      (Some(1), None, 4),
      (None, Some(9), 9)
    ).toDF("first", "second", "third")
    val expected = Seq(
      (1, 2, 3)
    ).toDF("first", "second", "third")
    val query = "subset=first,second"
    val actual = new OTLDropNA(SimpleQuery(query), utils).transform(source)
    assert(actual.except(expected).count() === 0)
  }

  test("Drop rows if all cols in a row contain missing values") {
    val source = Seq(
      (Some(1), Some(2), 3),
      (None, None, 4),
      (None, Some(9), 9)
    ).toDF("first", "second", "third")
    val expected = Seq(
      (Some(1), 2, 3),
      (None, 9, 9)
    ).toDF("first", "second", "third")
    val query = "subset=first,second how=all"
    val actual = new OTLDropNA(SimpleQuery(query), utils).transform(source)
    assert(actual.except(expected).count() === 0)
  }

  test("Drop rows with any cols with missing values without subset") {
    val source = Seq(
      (Some(1), Some(2), 3),
      (Some(1), None, 4),
      (None, Some(9), 9)
    ).toDF("first", "second", "third")
    val expected = Seq(
      (1, 2, 3)
    ).toDF("first", "second", "third")
    val query = "how=any"
    val actual = new OTLDropNA(SimpleQuery(query), utils).transform(source)
    assert(actual.except(expected).count() === 0)
  }

  test("Exception when subset contains columnds absent in df") {
    val source = Seq(
      (Some(1), Some(2), 3),
      (Some(1), None, 4),
      (None, Some(9), 9)
    ).toDF("first", "second", "third")
    val query = "subset=newcol,first"
    val thrown = intercept[Exception] {
      new OTLDropNA(SimpleQuery(query), utils).transform(source)
    }
    assert(thrown.getMessage.endsWith("not found in dataframe"))
  }

  test("Exception if incorrect 'how'") {
    val source = Seq(
      (Some(1), Some(2), 3),
      (Some(1), None, 4),
      (None, Some(9), 9)
    ).toDF("first", "second", "third")
    val query = "subset=first how=fail"
    val thrown = intercept[Exception] {
      new OTLDropNA(SimpleQuery(query), utils).transform(source)
    }
    assert(thrown.getMessage.startsWith("Incorrect value"))
  }
}