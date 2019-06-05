package com.pharbers.ipaas.data.driver.operators

import com.pharbers.ipaas.data.driver.api.work._

/**
  * @description:
  * @author: clock
  * @date: 2019-05-28 17:21
  */
case class select(plugin: PhPluginTrait, name: String, defaultArgs: PhWorkArgs[_]) extends PhOperatorTrait {

    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        val defaultMapArgs = defaultArgs.toMapArgs[PhWorkArgs[_]]
        val prMapArgs = pr.toMapArgs[PhWorkArgs[_]]
        val inDFName = defaultMapArgs.getAs[PhStringArgs]("inDFName").get.get
        val selectLst = defaultMapArgs.getAs[PhStringArgs]("selects").get.get.split("#")
        val inDF = prMapArgs.getAs[PhDFArgs](inDFName).get.get
        val outDF = inDF.selectExpr(selectLst: _*)

        PhDFArgs(outDF)
    }
}
