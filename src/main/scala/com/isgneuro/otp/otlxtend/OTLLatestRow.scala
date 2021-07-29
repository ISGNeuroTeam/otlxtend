package com.isgneuro.otp.otlxtend

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, max, lit, last}
import ot.dispatcher.sdk.core.{Positional, SimpleQuery}
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}

class OTLLatestRow(query: SimpleQuery, utils: PluginUtils) extends PluginCommand(query, utils, Set("by")) {
  private val timeCol = getKeyword("_time").getOrElse("_time")
  private val engine = getKeyword("engine").getOrElse("join")  // 'window' performs bad if no partitions specified
  private val byCols = positionalsMap.get("by") match {
    case Some(Positional("by", list)) => list.map(col)
    case _ => List(col("__internal__"))
  }

  override def transform(_df: DataFrame): DataFrame = {
    if (engine == "join")
      joinWithMaxTime(_df)
    else if (engine == "window")
      windowWithMaxTime(_df)
    else
      _df
  }

  private def joinWithMaxTime(df: DataFrame): DataFrame = {
    val dfExtraCols = df.withColumn("__internal__", lit(0))
    val dfMaxTime = dfExtraCols.groupBy(byCols: _*).agg(max(timeCol).as("__max__"))
    val dfJoined = dfExtraCols.join(dfMaxTime, byCols.map(_.toString))
    dfJoined.filter(s"${timeCol} == __max__")
      .drop("__internal__", "__max__")
  }

  private def windowWithMaxTime(df: DataFrame): DataFrame = {
    val window = Window.partitionBy(byCols: _*)
    df.withColumn("__max__", max(col(timeCol)).over(window).alias("__max__"))
      .filter(s""" ${timeCol} == __max__ """)
      .drop("__max__")
  }
}

