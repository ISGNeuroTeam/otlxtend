package com.isgneuro.otp.otlxtend

import org.apache.spark.sql.{DataFrame, SparkSession}
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

class OTLPitjoinTest extends CommandTest{
  val dataset = """[{}]"""

  private lazy val sparkSession: SparkSession =
    SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

  import sparkSession.implicits._

  private val left = Seq(
    ("2021-09-16 23:00:00", 101, 0),
    ("2021-09-17 01:15:30", 101, 1),
    ("2021-09-17 04:32:45", 101, 0),
    ("2021-09-17 05:43:10", 101, 1),
    ("2021-09-17 16:26:17", 101, 0)
  ).toDF("ts", "driver", "trip_success")

  private val right = Seq(
    ("2021-09-17 00:00:00", 101, 0, 0.00),
    ("2021-09-17 01:00:00", 101, 1, 50.00),
    ("2021-09-17 02:00:00", 101, 3, 142.00),
    ("2021-09-17 03:00:00", 101, 4, 184.00),
    ("2021-09-17 04:00:00", 101, 6, 238.00),
    ("2021-09-17 05:00:00", 101, 8, 301.00)
  ).toDF("ts", "driver", "trips_today", "earnings_today")

  test("Pit join with all params passed") {
    val expected = Seq(
      ("2021-09-16 23:00:00", 101, 0, "-1", -1, -1.00),
      ("2021-09-17 01:15:30", 101, 1, "2021-09-17 01:00:00", 1, 50.00),
      ("2021-09-17 04:32:45", 101, 0, "2021-09-17 04:00:00", 6, 238.00),
      ("2021-09-17 05:43:10", 101, 1, "2021-09-17 05:00:00", 8, 301.00),
      ("2021-09-17 16:26:17", 101, 0, "2021-09-17 05:00:00", 8, 301.00)
    ).toDF("ts", "driver", "trip_success", "r_ts", "r_trips_today", "r_earnings_today")

    val query = "driver subsearch=right left_ts=ts right_ts=ts right_prefix=r"
    val sq = SimpleQuery(query, Map("right" -> right))
    val actual = new OTLPitjoin(sq, utils)
      .transform(left)
      .na.fill(-1).na.fill("-1")

    actual.show()
    expected.show()

    assertDfEqual(actual, expected)
  }

  test("Pit join with default params") {
    val expected = Seq(
      ("2021-09-16 23:00:00", 101, 0, "-1", -1, -1.00),
      ("2021-09-17 01:15:30", 101, 1, "2021-09-17 01:00:00", 1, 50.00),
      ("2021-09-17 04:32:45", 101, 0, "2021-09-17 04:00:00", 6, 238.00),
      ("2021-09-17 05:43:10", 101, 1, "2021-09-17 05:00:00", 8, 301.00),
      ("2021-09-17 16:26:17", 101, 0, "2021-09-17 05:00:00", 8, 301.00)
    ).toDF("_time", "driver", "trip_success", "right__time", "right_trips_today", "right_earnings_today")

    val query = "driver subsearch=right"
    val sq = SimpleQuery(query, Map("right" -> right.withColumnRenamed("ts", "_time")))
    val actual = new OTLPitjoin(sq, utils)
      .transform(left.withColumnRenamed("ts", "_time"))
      .na.fill(-1).na.fill("-1")

    actual.show()
    expected.show()

    assertDfEqual(actual, expected)
  }

  test("Pit join with mutilple values in partitions") {
    val left = Seq(
      ("2021-09-16 23:00:00", 101, 0),
      ("2021-09-17 01:15:30", 200, 1),
      ("2021-09-17 04:32:45", 101, 0),
      ("2021-09-17 05:43:10", 101, 1),
      ("2021-09-17 16:26:17", 200, 0)
    ).toDF("ts", "driver", "trip_success")

    val right = Seq(
      ("2021-09-17 00:00:00", 101, 0, 0.00),
      ("2021-09-17 01:00:00", 101, 1, 50.00),
      ("2021-09-17 02:00:00", 101, 3, 142.00),
      ("2021-09-17 03:00:00", 101, 4, 184.00),
      ("2021-09-17 04:00:00", 101, 6, 238.00),
      ("2021-09-17 05:00:00", 101, 8, 301.00),
      ("2021-09-17 00:00:00", 200, 0, 1000.00),
      ("2021-09-17 01:00:00", 200, 11, 10050.00),
      ("2021-09-17 02:00:00", 200, 13, 100142.00),
      ("2021-09-17 03:00:00", 200, 14, 100184.00),
      ("2021-09-17 04:00:00", 200, 16, 100238.00),
      ("2021-09-17 05:00:00", 200, 18, 100301.00)
    ).toDF("ts", "driver", "trips_today", "earnings_today")

    val expected = Seq(
      ("2021-09-16 23:00:00", 101, 0, "-1", -1, -1.00),
      ("2021-09-17 01:15:30", 200, 1, "2021-09-17 01:00:00", 11, 10050.00),
      ("2021-09-17 04:32:45", 101, 0, "2021-09-17 04:00:00", 6, 238.00),
      ("2021-09-17 05:43:10", 101, 1, "2021-09-17 05:00:00", 8, 301.00),
      ("2021-09-17 16:26:17", 200, 0, "2021-09-17 05:00:00", 18, 100301.00)
    ).toDF("ts", "driver", "trip_success", "r_ts", "r_trips_today", "r_earnings_today")
      .sort("ts")

    val query = "driver subsearch=right left_ts=ts right_ts=ts right_prefix=r_"
    val sq = SimpleQuery(query, Map("right" -> right))
    val actual = new OTLPitjoin(sq, utils)
      .transform(left)
      .na.fill(-1).na.fill("-1")
      .sort("ts")

    actual.show()
    expected.show()

    assertDfEqual(actual, expected)
  }

  def assertDfEqual(first: DataFrame, second: DataFrame): Unit = {
    assert(first.except(second).count() == 0)
    assert(second.except(first).count() == 0)
  }
}
