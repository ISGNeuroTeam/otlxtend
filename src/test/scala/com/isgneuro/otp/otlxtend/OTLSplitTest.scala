package com.isgneuro.otp.otlxtend

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{exp, expr}
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

class OTLSplitTest extends CommandTest{
  private lazy val sparkSession: SparkSession =
    SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()
  import sparkSession.implicits._

  val dataset = """[{}]"""

  test("Get keywords") {
    val source = sparkSession.range(2).toDF("id")
      .withColumn("text", expr("""if(id == 0, "col1-col2-col3", "col11-col22-col33")"""))
    val expected = Seq((0, "col1", "col2", "col3"), (1, "col11", "col22", "col33")).toDF("id", "col1", "col2", "col3")
    val query = "text cols=col1,col2,col3 sep=-"
    val actual = new OTLSplit(SimpleQuery(query), utils).transform(source)
    assert(actual.columns.sorted === Array("col1", "col2", "col3", "id", "text"))
  }

  test("Correct split") {
    val source = sparkSession.range(2).toDF("id")
      .withColumn("text", expr("""if(id == 0, "col1-col2-col3", "col11-col22-col33")"""))
    val expected = Seq(
      (0, "col1-col2-col3", "col1", "col2", "col3"),
      (1, "col11-col22-col33", "col11", "col22", "col33")
    ).toDF("id", "text", "col1", "col2", "col3")
    val query = """text cols=col1,col2,col3 sep=-"""
    val actual = new OTLSplit(SimpleQuery(query), utils).transform(source)
    assert(actual.except(expected).count() === 0)
  }
}