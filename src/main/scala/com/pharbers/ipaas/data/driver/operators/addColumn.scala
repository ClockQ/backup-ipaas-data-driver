package com.pharbers.ipaas.data.driver.operators

import org.apache.spark.sql.{Column, DataFrame}
import com.pharbers.ipaas.data.driver.api.work._

/**
  * @description:
  * @author: clock
  * @date: 2019-05-28 17:21
  */
case class addColumn(name: String, args: PhMapArgs[PhWorkArgs[Any]], pluginLst: Seq[PhPluginTrait2[Column]]) extends PhOperatorTrait2[DataFrame] {
    val inDFName: String = args.getAs[PhStringArgs]("inDFName").get.get
    val newColName: String = args.getAs[PhStringArgs]("newColName").get.get

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[DataFrame] = {
        val inDF = pr.getAs[PhDFArgs](inDFName).get.get
        val plugin = pluginLst.head.perform(pr).get
        PhDFArgs(inDF.withColumn(newColName, plugin))
    }
}
