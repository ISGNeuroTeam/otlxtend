package com.isgneuro.otp.otlxtend

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, last, lit}
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}
import ot.dispatcher.sdk.core.extensions.StringExt._

/** Perform a backward asof join using the left table for event times.
 * Credits: https://github.com/Ackuq/spark-pit/ (cannot use directly because of only Spark 3.2+ support)
 *
 * Keywords:
 * @param left_ts
 * The column used for timestamps in left DF
 * @param right_ts
 * The column used for timestamps in right DF
 * @param left_prefix
 * Optional, the prefix for the left columns in result
 * @param right_prefix
 * The prefix for the right columns in result
 *
 * @return
 * The PIT-correct view of the joined dataframes
 */

class OTLPitjoin(query: SimpleQuery, utils: PluginUtils) extends PluginCommand(query, utils, Set("by")) {
  val cachedDfs: Map[String, DataFrame] = query.cache

  /** Intermediate column names
   */
  private val DF_INDEX_COLUMN = "df_index"
  private val COMBINED_TS_COLUMN = "df_combined_ts"

  override def transform(left: DataFrame): DataFrame = {
    val subsearch: String = getKeyword("subsearch").getOrElse("__nosubsearch__")

    if (returns.flatFields.isEmpty || !cachedDfs.contains(subsearch)) {
      return left
    }

    val right: DataFrame = cachedDfs(subsearch)

    val leftTSColumn: String = getKeyword("left_ts").getOrElse("_time")
    val rightTSColumn: String = getKeyword("right_ts").getOrElse("_time")
    val leftPrefix: Option[String] = getKeyword("left_prefix")
    val rightPrefix: String = getKeyword("right_prefix").getOrElse("right_")
    val partitionCols: Seq[String] = returns.flatFields.map(_.stripBackticks())

    val bothCols = left.columns.intersect(right.columns)
    if (!partitionCols.forall(bothCols.contains)) {
      utils.sendError(query.searchId, "Not all partition columns exist in both dataframes")
    }

    val leftPrefixed = leftPrefix match {
      case Some(lp) => prefixDF(left, lp, partitionCols)
      case None => left
    }
    val rightPrefixed = prefixDF(right, rightPrefix, partitionCols)

    // Timestamp columns
    val leftTS = leftPrefix match {
      case Some(p) => p ++ leftTSColumn
      case None => leftTSColumn
    }
    val rightTS = rightPrefix ++ rightTSColumn

    // Add all the column to both dataframes
    val leftPrefixedAllColumns = addColumns(
      leftPrefixed.withColumn(DF_INDEX_COLUMN, lit(1)),
      rightPrefixed.columns.filter(!partitionCols.contains(_))
    )
    val rightPrefixedAllColumns =
      addColumns(
        rightPrefixed.withColumn(DF_INDEX_COLUMN, lit(0)),
        leftPrefixed.columns.filter(!partitionCols.contains(_))
      )

    // Combine the dataframes
    val combined = leftPrefixedAllColumns.unionByName(rightPrefixedAllColumns)

    // Get the combined TS from the dataframes, this will be used for ordering
    val combinedTS = combined.withColumn(
      COMBINED_TS_COLUMN,
      coalesce(combined(leftTS), combined(rightTS))
    )

    val windowSpec = Window
      .orderBy(COMBINED_TS_COLUMN, DF_INDEX_COLUMN)
      .partitionBy(
        partitionCols.map(col): _*
      )
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    // Get all non-partitioning columns
    val rightColumns = rightPrefixed.columns.filter(!partitionCols.contains(_))

    // To perform the join, a window is slides through all the rows and merges each row with the last right values
    val asOfDF =
      rightColumns
        .foldLeft(combinedTS)((df, col) =>
          df.withColumn(col, last(df(col), ignoreNulls = true).over(windowSpec))
        )
        // Invalid candidates are those where the left values are not existing
        .filter(col(leftTS).isNotNull)
//        .filter(col(rightTS).isNotNull)
        .drop(DF_INDEX_COLUMN)
        .drop(COMBINED_TS_COLUMN)

    asOfDF
  }

  private def prefixDF(
                df: DataFrame,
                prefix: String,
                ignoreColumns: Seq[String]
              ): DataFrame = {
    val newColumnsQuery = df.columns.map(col =>
      if (ignoreColumns.contains(col)) df(col) else df(col).as(prefix ++ col)
    )
    df.select(newColumnsQuery: _*)
  }

  private def addColumns(df: DataFrame, columns: Seq[String]) = {
    columns.foldLeft(df)(_.withColumn(_, lit(null)))
  }
}
