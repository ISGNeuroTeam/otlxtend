package com.isgneuro.otp.otlxtend

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, split}
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.core.extensions.StringExt._
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}

class OTLSplit (query: SimpleQuery, utils: PluginUtils) extends PluginCommand(query, utils, Set.empty[String]) {

  def transform(_df: DataFrame): DataFrame = {
    val cols = getKeyword("cols").map(_.split(",").map(_.trim))
    val sep = getKeyword("sep").getOrElse("#")
    returns.flatFields.headOption.map(_.stripBackticks) match {
      case Some(colname) =>
        val tempDf = _df.withColumn("temp", split(col(colname), sep))
        cols.map(
          _.zipWithIndex.foldLeft(tempDf) {
            case(acc, (name, idx)) => acc.withColumn(name, col("temp").getItem(idx).as(name))
          }.drop("temp")
        ).getOrElse(_df)
      case _ => sendError("Column name is not specified")
    }
  }
}
