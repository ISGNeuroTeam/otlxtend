package ot.dispatcher.plugins.additionalcommands.commands

import java.io.File

import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}

import scala.util.{Failure, Success, Try}
/** SMaLL command. It changes number of partition in dataframe.
 * @param sq [[SimpleQuery]] search query object.
 * @author Sergey Ermilov (sermilov@ot.ru)
 */
class Coalesce(sq: SimpleQuery, utils: PluginUtils) extends PluginCommand(sq, utils) {
  import utils._
  val requiredKeywords: Set[String] = Set("num")

  def transform(_df: DataFrame): DataFrame = 
  {
    val numPartitions = Try(getKeyword("num").get) match {
       case Success(x) => x
       case Failure(_)=> sendError("The value of parameter 'num' should be specified")

    }
    val num = Try(numPartitions.toInt) match {
      case Success(x) => x
      case Failure(_) => sendError("The value of parameter 'num' should be of int type")
    }
   _df.coalesce(num)
  }
}
