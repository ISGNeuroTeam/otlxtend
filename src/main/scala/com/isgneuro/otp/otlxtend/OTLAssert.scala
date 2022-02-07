package com.isgneuro.otp.otlxtend

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}

import scala.util.{Failure, Success, Try}

class OTLAssert (query: SimpleQuery, utils: PluginUtils) extends PluginCommand(query, utils, Set.empty[String]) {
  log.info(s"args: $args")

  def transform(_df: DataFrame): DataFrame = {
    Try { _df.filter(expr(args.trim)) } match {
      case Success(dfFiltered) if _df.count() == dfFiltered.count() => _df
      case Success(_) => sendError(s"Assertion rule ${args.trim} failed")
      case Failure(msg) => sendError(s"Failed to parse assert expression $args. Exception message: $msg")
    }
  }
}
