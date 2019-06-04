package com.pharbers.ipaas.data.driver.operators

import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhOperatorTrait, PhPluginTrait, PhStringArgs, PhWorkArgs}
import org.apache.spark.sql.functions.expr

/**
  * @author dcs
  * @param $args
  * @tparam T
  * @note
  */
case class groupOperator(plugin: PhPluginTrait, name: String, defaultArgs: PhWorkArgs[_]) extends PhOperatorTrait {

    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        val defaultMapArgs = defaultArgs.toMapArgs[PhWorkArgs[_]]
        val prMapArgs = pr.toMapArgs[PhWorkArgs[_]]
        val inDFName = defaultMapArgs.getAs[PhStringArgs]("inDFName").get.get
        val groupCol = defaultMapArgs.getAs[PhStringArgs]("groupCol").get.get
        val agg = defaultMapArgs.getAs[PhStringArgs]("agg").get.get.split(",").map(x => expr(x))
        val inDF = prMapArgs.getAs[PhDFArgs](inDFName).get.get
        val outDF = inDF.groupBy(groupCol).agg(agg.head ,agg.tail: _*)
        PhDFArgs(outDF)
    }
}
