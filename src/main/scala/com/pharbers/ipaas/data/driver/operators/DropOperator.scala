package com.pharbers.ipaas.data.driver.operators

import com.pharbers.ipaas.data.driver.api.work._

/**
  * @author dcs
  * @param $args
  * @tparam T
  * @note
  */
case class DropOperator(plugin: PhPluginTrait, name: String, defaultArgs: PhWorkArgs[_]) extends PhOperatorTrait {

    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        val defaultMapArgs = defaultArgs.toMapArgs[PhWorkArgs[_]]
        val prMapArgs = pr.toMapArgs[PhWorkArgs[_]]
        val inDFName = defaultMapArgs.getAs[PhStringArgs]("inDFName").get.get
        val dropColName = defaultMapArgs.getAs[PhStringArgs]("dropColName").get.get
        val inDF = prMapArgs.getAs[PhDFArgs](inDFName).get.get
        val outDF = inDF.drop(dropColName)

        PhDFArgs(outDF)
    }
}