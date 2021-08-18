package com.isgneuro.otp.otlxtend

import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}
import ot.dispatcher.sdk.core.SimpleQuery

class OTLComment(query: SimpleQuery, utils: PluginUtils) extends PluginCommand(query, utils, Set.empty[String]){
  override val fieldsUsed = List.empty[String]
  override def transform(_df: DataFrame): DataFrame = _df
}
