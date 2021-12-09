package com.isgneuro.otp.otlxtend

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array, col, explode, typedLit}
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}
import ot.dispatcher.sdk.core.SimpleQuery

class ConnectedComponents(query: SimpleQuery, utils: PluginUtils)
  extends PluginCommand(query, utils, Set.empty[String]) {

  private val srcCol: String = getKeyword("src").getOrElse("src")
  private val dstCol: String = getKeyword("dst").getOrElse("dst")

  override def transform(_df: DataFrame): DataFrame = {

    val edges: RDD[Edge[String]] = _df
      .select(
        col(srcCol).cast("long"),
        col(dstCol).cast("long"))
      .rdd
      .map(
        row => Edge(
          row.getAs[Long](0),
          row.getAs[Long](1),
          "fake property"
        )
      )

    val graph = Graph.fromEdges(edges, "fake vertex prop")
    val cc = graph.connectedComponents()
    val ccMap = typedLit(
      cc.vertices.collect().toMap
    )

    _df.withColumn("cc", ccMap(col(srcCol)))
  }
}
