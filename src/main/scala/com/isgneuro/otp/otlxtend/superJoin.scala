package com.isgneuro.otp.otlxtend

import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

class superJoin(sq: SimpleQuery, utils: PluginUtils) extends PluginCommand(sq, utils) {
   import utils._
   val requiredKeywords: Set[String] = Set("type", "format", "path")

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

