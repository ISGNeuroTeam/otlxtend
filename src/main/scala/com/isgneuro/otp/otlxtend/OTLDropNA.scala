package com.isgneuro.otp.otlxtend

import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}
import ot.dispatcher.sdk.core.SimpleQuery

/** Drop rows which contain missing values.
 *
 * Keywords:
 * > subset - Columns to consider
 * > how - Determine if column is removed from DataFrame, when we have at least one NA or all NA.
 *    ‘any’ : If any NA values are present, drop that row or column.
 *    ‘all’ : If all values are NA, drop that row or column.
 *
 * Command example:
 * > | drop subset=cpu_util,mem_used how=all
 *
 * @param query
 * @param utils
 */
class OTLDropNA(query: SimpleQuery, utils: PluginUtils) extends PluginCommand(query, utils, Set.empty) {
  val HOW_DEFAULT: String = "any"

  override def transform(_df: DataFrame): DataFrame = {
    val subset: Array[String] =
      getKeyword("subset")
        .map(c => c.split(",").map(_.trim))
        .getOrElse(_df.columns)

    subset.diff(_df.columns) match {
      case cols if cols.nonEmpty => sendError(s"""Columns ${cols.mkString(", ")} not found in dataframe""")
      case _ =>
    }

    val how: String =
      getKeyword("how").getOrElse(HOW_DEFAULT) match {
        case "any" => "any"
        case "all" => "all"
        case _ => sendError("Incorrect value of 'how' keyword")
      }

    _df.na.drop(how, subset)
  }
}
