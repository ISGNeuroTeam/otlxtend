package com.isgneuro.otp.otlxtend

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{last, lit}
import ot.dispatcher.sdk.core.extensions.StringExt._
import ot.dispatcher.sdk.core.{Positional, SimpleQuery}
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}

class OTLLatest (query: SimpleQuery, utils: PluginUtils) extends PluginCommand(query, utils, Set("by")) {

  def transform(_df: DataFrame): DataFrame = {
    val fields = returns.flatFields.map(c => c.stripBackticks)
    val dfGrouped = positionalsMap.get("by") match {
      case Some(Positional("by", posHead :: posTail)) => _df.groupBy(posHead, posTail: _*)
      case _ => _df.withColumn("__internal__", lit(0)).groupBy("__internal__")
    }
    fields.map(col => last(col, ignoreNulls=true).alias(col)) match {
      case h :: t => dfGrouped.agg(h, t: _*).drop("__internal__")
      case _ => _df
    }
  }
}
