package com.isgneuro.otp.otlxtend

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{col, last, lit, max}
import ot.dispatcher.sdk.core.{Positional, SimpleQuery}
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}

class OTLLatestRow(query: SimpleQuery, utils: PluginUtils) extends PluginCommand(query, utils, Set("by")) {
  // Extract keywords and positional args
  private val timeCol: String = getKeyword("_time").getOrElse("_time")
  private val engine: String = getKeyword("engine").getOrElse("join")  // 'window' performs bad if no partitions specified

  private val byCols: List[Column] = positionalsMap.get("by") match {
    case Some(Positional("by", list)) => list.map(col)
    case _ => List(col("__internal__"))
  }

  // Transformation elements
  private val transformMap: Map[String, DataFrame => DataFrame] =
    Map(
      "join" -> joinWithMaxTime,
      "window" -> windowWithMaxTime
    )

  private def identityDF(_df: DataFrame): DataFrame = _df

  private def joinWithMaxTime(df: DataFrame): DataFrame = {
    val dfExtraCols: DataFrame = df.withColumn("__internal__", lit(0))

    val dfMaxTime: DataFrame =
      dfExtraCols
        .groupBy(byCols: _*)
        .agg(max(timeCol).as("__max__"))

    val dfJoined: DataFrame = dfExtraCols.join(dfMaxTime, byCols.map(_.toString))

    dfJoined
      .filter(s"$timeCol == __max__")
      .drop("__internal__", "__max__")
  }

  private def windowWithMaxTime(df: DataFrame): DataFrame = {
    val window: WindowSpec = Window.partitionBy(byCols: _*)

    df.withColumn("__max__", max(col(timeCol)).over(window).alias("__max__"))
      .filter(s""" $timeCol == __max__ """)
      .drop("__max__")
  }

  override def transform(_df: DataFrame): DataFrame = {
    _df.transform(
      transformMap.getOrElse(engine, identityDF)
    )
  }
}
