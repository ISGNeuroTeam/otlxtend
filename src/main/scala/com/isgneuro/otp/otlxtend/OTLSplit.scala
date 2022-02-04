package com.isgneuro.otp.otlxtend

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, split}
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.core.extensions.StringExt._
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}

class OTLSplit (query: SimpleQuery, utils: PluginUtils) extends PluginCommand(query, utils, Set.empty[String]) {
  val cols: Option[Array[String]] = getKeyword("cols").map(_.split(",").map(_.trim))
  log.info(s"cols: ${cols.map(_.mkString("Array(", ", ", ")"))}")

  val sep: String = getKeyword("sep").getOrElse("#")
  log.info(s"sep: $sep")

  def transform(_df: DataFrame): DataFrame = {
    val xz =
      returns
        .flatFields
        .headOption
        .map(_.stripBackticks)

    returns.flatFields.headOption.map(_.stripBackticks) match {
      case Some(colname) =>
        val tempDf: DataFrame = _df.withColumn("temp", split(col(colname), sep))

        cols
          .map { column =>
            column
              .zipWithIndex
              .foldLeft(tempDf) { case (acc, (name, idx)) =>
                acc.withColumn(name, col("temp").getItem(idx).as(name))
              }
              .drop("temp")
          }
          .getOrElse(_df)

      case _ =>
        sendError("Column name is not specified")
    }
  }
}
