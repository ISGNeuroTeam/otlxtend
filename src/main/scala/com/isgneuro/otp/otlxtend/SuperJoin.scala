package com.isgneuro.otp.otlxtend

import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

class SuperJoin(sq: SimpleQuery, utils: PluginUtils) extends PluginCommand(sq, utils) {
   val requiredKeywords: Set[String] = Set("type", "format", "path")

  val joinType: String = Try(getKeyword("type").get) match {
    case Success(x) => x
    case Failure(_) => sendError("The value of parameter 'type' should be specified")
  }

  log.info(s"joinType: $joinType")

  val path: String = Try(getKeyword("path").get) match {
    case Success(x) => x
    case Failure(_) => sendError("The value of parameter 'path' should be specified")
  }

  log.info(s"path: $path")

  val format: String = Try(getKeyword("format").get) match {
    case Success(x) => x
    case Failure(_) => sendError("The value of parameter 'path' should be specified")
  }

  log.info(s"format: $format")

  val joinOn: List[String] =
    returns
      .flatFields
      .map(_.stripPrefix("`").stripSuffix("`"))

  log.info(s"joinOn: $joinOn")

   def transform(_df: DataFrame): DataFrame = {
     val sparkSession: SparkSession = SparkSession.builder().getOrCreate()

     val jdf: DataFrame =
       sparkSession
         .read
         .format(format)
         .option("header", "true")
         .load(path)
     
     val joinedDf: DataFrame = _df.join(jdf, joinOn, joinType)
     joinedDf
   }
}

