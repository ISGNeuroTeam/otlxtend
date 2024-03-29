package com.isgneuro.otp.otlxtend

import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}
import org.apache.spark.sql.functions.{last, lit}
import ot.dispatcher.sdk.core.extensions.StringExt._
import ot.dispatcher.sdk.core.{Field, Positional, SimpleQuery}
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}

class OTLLatest (query: SimpleQuery, utils: PluginUtils) extends PluginCommand(query, utils, Set("by")) {
  val fields: List[String] =
    returns
      .flatFields
      .map(c => c.stripBackticks)

  log.info(s"fields: $fields")

  def transform(_df: DataFrame): DataFrame = {
    val posMapBy: Option[Field] = positionalsMap.get("by")
    log.info(s"posMapBy: $posMapBy")

    val dfGrouped: RelationalGroupedDataset =
      posMapBy match {
        case Some(Positional("by", posHead :: posTail)) =>
          _df.groupBy(posHead, posTail: _*)

        case _ =>
          _df
            .withColumn("__internal__", lit(0))
            .groupBy("__internal__")
      }

    fields.map(col => last(col, ignoreNulls = true).alias(col)) match {
      case h :: t =>
        dfGrouped
          .agg(h, t: _*)
          .drop("__internal__")

      case _ =>
        _df
    }
  }
}
