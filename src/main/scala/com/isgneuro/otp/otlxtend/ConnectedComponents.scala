package com.isgneuro.otp.otlxtend

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}
import ot.dispatcher.sdk.core.SimpleQuery

class ConnectedComponents(query: SimpleQuery, utils: PluginUtils)
  extends PluginCommand(query, utils, Set.empty[String]) {

  private val srcCol: String = getKeyword("src").getOrElse("src")
  log.info(s"srcCol: $srcCol")

  private val dstCol: String = getKeyword("dst").getOrElse("dst")
  log.info(s"dstCol: $srcCol")

  override def transform(_df: DataFrame): DataFrame = {
    val edges: RDD[Edge[String]] =
      _df
        .select(
          col(srcCol).cast("long"),
          col(dstCol).cast("long")
        )
        .rdd
        .map { row =>
          Edge(
            row.getAs[Long](0),
            row.getAs[Long](1),
            "fake property"
          )
        }

    val graph: Graph[String, String] = Graph.fromEdges(edges, "fake vertex prop")
    val cc: Graph[VertexId, String] = graph.connectedComponents()

    _df.join(
      _df.sparkSession.createDataFrame(cc.vertices).toDF(srcCol, "cc"),
      usingColumn = srcCol
    )
  }
}
