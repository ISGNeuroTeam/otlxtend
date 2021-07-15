package com.isgneuro.otp.otlxtend

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, first, lit}
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}
import ot.dispatcher.sdk.core.SimpleQuery

class OTLPivot(query: SimpleQuery, utils: PluginUtils) extends PluginCommand(query, utils) {
  override def transform(_df: DataFrame): DataFrame = {
    returns.flatFields.reverse match {
      case values :: groups :: fixed =>
        _df.groupBy(fixed.map(col): _*)
          .pivot(groups)
          .agg(first(values))
      case values :: groups :: _ =>
        _df.withColumn("__id__", lit(1))
          .groupBy("__id__")
          .pivot(groups)
          .agg(first(values))
          .drop("__id__")
      case _ => _df
    }
  }
}