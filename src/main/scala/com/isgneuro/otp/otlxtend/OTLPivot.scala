package com.isgneuro.otp.otlxtend

import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}
import org.apache.spark.sql.functions.{col, first, lit}
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}
import ot.dispatcher.sdk.core.SimpleQuery

case class PivotParams(valuesCol: String, categoriesCol: String, fixedCols: Option[List[String]])

class OTLPivot(query: SimpleQuery, utils: PluginUtils) extends PluginCommand(query, utils) {
  private val categories: Option[Array[String]] =
    getKeyword("groups").map(_.replaceAllLiterally(" ", "").split(","))

  val params: PivotParams =
    returns
      .flatFields
      .reverse match {
      case values :: groups :: fixed => PivotParams(values, groups, Some(fixed.reverse))
      case values :: groups :: Nil => PivotParams(values, groups, None)
      case _ => sendError("You should specify at least values column and category column using 'pivot'")
    }

  private def group(df: DataFrame): RelationalGroupedDataset =
    params.fixedCols match {
      case Some(cols) =>
        df.groupBy(cols.map(col): _*)

      case _ =>
        df
          .withColumn("__id__", lit(1))
          .groupBy("__id__")
    }

  private def pivot(rgd: RelationalGroupedDataset): RelationalGroupedDataset =
    categories match {
      case Some(cats) => rgd.pivot(params.categoriesCol, cats)
      case _ => rgd.pivot(params.categoriesCol)
    }

  private def aggregate(rgd: RelationalGroupedDataset): DataFrame =
    rgd.agg(first(params.valuesCol))

  private def dropIdCol(df: DataFrame): DataFrame =
    if (df.columns.contains("__id__")) df.drop("__id__") else df

  private def transformFunc: DataFrame => DataFrame =
    group _ andThen
      pivot andThen
      aggregate andThen
      dropIdCol

  override def transform(_df: DataFrame): DataFrame = transformFunc(_df)
}