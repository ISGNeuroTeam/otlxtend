package com.isgneuro.otp.otlxtend

import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.SparkSession
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

class OTLLatestTest extends CommandTest {
  private lazy val sparkSession: SparkSession =
    SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()
  import sparkSession.implicits._

  val dataset = """[{}]"""

  test("Get latest not null element") {
    val source = sparkSession.range(10).toDF("id")
      .withColumn("mod3", expr("""if(id % 3 == 0, null, id)"""))
      .withColumn("mod2", expr("""if(id % 2 == 0, null, id)"""))
    val expected = Seq((9, 8)).toDF("mod2", "mod3")
    val query = "mod2, mod3"
    val actual = new OTLLatest(SimpleQuery(query), utils).transform(source)
    source.show()
    expected.show()
    actual.show()
    assert(actual.except(expected).count() == 0)
  }

  test("Get latest not null element with by statement") {
    val source = sparkSession.range(10).toDF("id")
      .withColumn("mod3", expr("""if(id % 3 == 0, null, id)"""))
      .withColumn("mod2", expr("""if(id % 2 == 0, null, id)"""))
      .withColumn("cat", expr("""if(id < 5, "small", "big")"""))
    val expected = Seq(("big", 9, 8), ("small", 3, 4)).toDF("cat", "mod2", "mod3")
    val query = "mod2, mod3 by cat"
    val actual = new OTLLatest(SimpleQuery(query), utils).transform(source)
    source.show()
    expected.show()
    actual.show()
    assert(actual.except(expected).count() == 0)
  }
}