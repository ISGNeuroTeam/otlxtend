package com.isgneuro.otp.otlxtend

import org.apache.spark.sql.SparkSession
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

class OTLUrlTest extends CommandTest{
  private lazy val sparkSession: SparkSession =
    SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()
  import sparkSession.implicits._

  val dataset = """[{}]"""

  test("Convert url string to normal string") {
    val source = Seq(
      (1, "%7C%20readFile%20format%3Dparquet%20path%3Dlast_mont%7C%20where%20_time%20%3E%201554076800%7C%20where%20_time%20%3C%201559260800%7C%20apply%20graphview%7C%20eval%20Avg_sec%20%3D%20edge_description"),
      (2, "%7C%20join%20type%3Dleft%20relation%20%5B%7C%20inputlookup%20bpm_1month_statistics2.csv%20%7C%20rename%20node%20as%20relation"),
      (3, "%7C%20rename%20percent%20as%20rel_percent%20%7C%20rename%20sum_perc%20as%20rel_sum_perc%5D%7C%20eval%20rel_sum_perc%20%3D%20if(relation%20%3D%20%22start%22%2C%200%2C%20rel_sum_perc)")
    ).toDF("ind", "url_text")
    val expected = Seq(
      (1, "| readFile format=parquet path=last_mont| where _time > 1554076800| where _time < 1559260800| apply graphview| eval Avg_sec = edge_description"),
      (2, "| join type=left relation [| inputlookup bpm_1month_statistics2.csv | rename node as relation"),
      (3, "| rename percent as rel_percent | rename sum_perc as rel_sum_perc]| eval rel_sum_perc = if(relation = \"start\", 0, rel_sum_perc)")
    ).toDF("ind", "decode_url_text")
    val query = "url_text"
    val actual = new OTLUrl(SimpleQuery(query), utils).transform(source).drop("url_text")
    assert(actual.except(expected).count() === 0)
  }

}