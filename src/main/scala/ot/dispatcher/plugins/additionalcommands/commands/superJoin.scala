package ot.dispatcher.plugins.additionalcommands.commands

import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

class superJoin(sq: SimpleQuery, utils: PluginUtils) extends PluginCommand(sq, utils) {
   import utils._
   val requiredKeywords: Set[String] = Set("type", "format", "path")
// join id type=left path=/kek/kek/ format=parquet
   def transform(_df: DataFrame): DataFrame = 
   {
     val join_type = Try(getKeyword("type").get) match {
         case Success(x) => x
         case Failure(_) => sendError("The value of parameter 'type' should be specified")
     }
     val path = Try(getKeyword("path").get) match {
         case Success(x) => x
         case Failure(_) => sendError("The value of parameter 'path' should be specified")
     }
     val format = Try(getKeyword("format").get) match {
         case Success(x) => x
         case Failure(_) => sendError("The value of parameter 'path' should be specified")
     }
     val joinOn = returns.flatFields.toSeq.map(_.stripPrefix("`").stripSuffix("`"))

     val sparkSession = SparkSession.builder().getOrCreate()      
     val jdf = sparkSession.read.format(format).option("header", "true").load(path) 
     
     val joinedDf = _df.join(jdf, joinOn, join_type) 
     joinedDf
   }
}

/*class SplJoin(sq: SimpleQuery) extends SplBaseCommand(sq) {
  val requiredKeywords= Set.empty[String]
  val optionalKeywords= Set("subsearch","max","type")
  val cachedDFs = sq.cache
  override def transform(_df: DataFrame): DataFrame = {
    val subsearch: String = getKeyword("subsearch").getOrElse("__nosubsearch__")
    if (cachedDFs.contains(subsearch) && returns.flatFields.nonEmpty) {
      val jdf: DataFrame = cachedDFs(subsearch)
      val joinOn = returns.flatFields.toSeq.map(_.stripBackticks)
      val bothCols = _df.columns.intersect(jdf.columns)
      log.debug(f"[SearchId:${sq.searchId}] Columns in right df: ${jdf.columns.mkString(", ")}")
      log.debug(f"[SearchId:${sq.searchId}] Columns join on: ${joinOn.mkString(", ")}")
      log.debug(f"[SearchId:${sq.searchId}] Columns in both dashboards: ${bothCols.mkString(", ")}")
      if (joinOn.forall(bothCols.contains)) {
        val dfSuffix = if (jdf.isEmpty) _df //TODO find more convenient vay to check if df is empty
        else bothCols.diff(joinOn).foldLeft(_df) { case (accum, item) =>
          accum.drop(item)
        val intdf = dfSuffix.join(jdf, joinOn, getKeyword("type").getOrElse("left"))
        if (getKeyword("max").getOrElse("0") == "1") {
          intdf.dropDuplicates(joinOn)
        } else intdf
      } else _df
    } else _df
  }
}
*/
