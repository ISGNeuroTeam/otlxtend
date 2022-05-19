package com.isgneuro.otp.otlxtend

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.core.extensions.StringExt._
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}

import java.nio.charset.StandardCharsets

/** Drop rows which contain missing values.
 *
 * Keywords:
 * > subset - Columns to consider
 * > how - Determine if column is removed from DataFrame, when we have at least one NA or all NA.
 * ‘any’ : If any NA values are present, drop that row or column.
 * ‘all’ : If all values are NA, drop that row or column.
 *
 * Command example:
 * > | drop subset=cpu_util,mem_used how=all
 *
 * @param query
 * @param utils
 */
class OTLUrl(query: SimpleQuery, utils: PluginUtils) extends PluginCommand(query, utils, Set.empty) {
  val fields: List[String] =
    returns
      .flatFields
      .map(c => c.stripBackticks)
  println(fields)

  log.info(s"fields: $fields")
  val decode: String => String = (inputString: String) => java.net.URLDecoder.decode(inputString, StandardCharsets.UTF_8.name())
  val decode_udf: UserDefinedFunction = udf(decode)

  def transform(_df: DataFrame): DataFrame = {
    fields.foldLeft(_df)((input_df, field) => input_df.withColumn("decode_" + field, decode_udf(col(field))))
  }

}
