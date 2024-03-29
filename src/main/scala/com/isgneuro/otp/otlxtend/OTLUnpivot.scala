package com.isgneuro.otp.otlxtend

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode, expr}
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.core.extensions.StringExt._

/**
 * Transforms wide table to long. User should specify any number of fixed columns.
 * All other columns are transform to two. Names of these two columns are also must be specified.
 *
 * Ex.:
 * input: table with columns _time, well, m1, m2, m3
 * query: | untable _time, well, metric_name, value
 * output: table with columns _time, well, metric_name, value
 *
 * @param query - SimpleQuery object corresponding to query text
 * @param utils - plugin utils
 */
class OTLUnpivot(query: SimpleQuery, utils: PluginUtils) extends PluginCommand(query, utils) {
  val fieldsInQuery: List[String] =
    returns
      .flatFields
      .map(_.stripBackticks)

  log.info(s"fieldsInQuery: $fieldsInQuery")

  override def transform(_df: DataFrame): DataFrame = {
    val actualCols: Array[String] =
      _df
        .columns
        .filterNot(fieldsInQuery.contains)

    log.info(s"actualCols: ${actualCols.mkString("Array(", ", ", ")")}")

    fieldsInQuery.reverse match {
      case value :: group :: fixed =>
        actualCols
          .foldLeft(_df) { (accum, colname) =>
            accum.withColumn(colname, expr(s"""array("$colname", $colname)"""))
          }
          .withColumn("arr", expr(s"""array(${actualCols.mkString(", ")})"""))
          .select("arr", fixed: _*)
          .withColumn("arr", explode(col("arr")))
          .withColumn(group.strip("\"").stripBackticks, col("arr").getItem(0))
          .withColumn(value.strip("\"").stripBackticks, col("arr").getItem(1))
          .drop("arr")

      case _ => _df
    }
  }
}