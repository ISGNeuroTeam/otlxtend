package com.isgneuro.otp.otlxtend

import java.io.File

import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}

import scala.util.{Failure, Success, Try}
/** SMaLL command. It changes number of partition in dataframe by repartitioning.
 * @param sq [[SimpleQuery]] search query object.
 * @author Sergey Ermilov (sermilov@ot.ru)
 */
class Repartition(sq: SimpleQuery, utils: PluginUtils) extends PluginCommand(sq, utils) {
  val requiredKeywords: Set[String] = Set("num")

  val numPartitions: Int =
    Try { getKeyword("num").map(_.toInt).get } match {
      case Success(x) if x > 0 => x
      case Failure(_) => sendError("You should specify the 'num' parameter as positive integer")
    }

  def transform(_df: DataFrame): DataFrame = {
   _df.repartition(numPartitions)
  }
}
