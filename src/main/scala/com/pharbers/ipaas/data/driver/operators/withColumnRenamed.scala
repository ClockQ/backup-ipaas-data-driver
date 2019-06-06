package com.pharbers.ipaas.data.driver.operators

import com.pharbers.ipaas.data.driver.api.work._
import org.apache.spark.sql.functions._
/**
  * @description:
  * @author: clock
  * @date: 2019-05-28 17:21
  */
case class withColumnRenamed(plugin: PhPluginTrait, name: String, defaultArgs: PhWorkArgs[_]) extends PhOperatorTrait {

    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        val defaultMapArgs = defaultArgs.toMapArgs[PhWorkArgs[_]]
        val prMapArgs = pr.toMapArgs[PhWorkArgs[_]]
        val inDFName = defaultMapArgs.getAs[PhStringArgs]("inDFName").get.get
        val oldColName = defaultMapArgs.getAs[PhStringArgs]("oldColName").get.get
        val newColName = defaultMapArgs.getAs[PhStringArgs]("newColName").get.get
        val inDF = prMapArgs.getAs[PhDFArgs](inDFName).get.get
        val outDF = inDF.withColumnRenamed(oldColName, newColName)

        PhDFArgs(outDF)
    }
}
