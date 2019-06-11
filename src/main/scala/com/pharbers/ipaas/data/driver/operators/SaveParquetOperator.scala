package com.pharbers.ipaas.data.driver.operators

import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhMapArgs, PhNoneArgs, PhOperatorTrait, PhPluginTrait, PhStringArgs, PhWorkArgs}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.libs.spark.util.{readCsv, save2Parquet}
import env.sparkObj

/** 功能描述
  *
  * @author EDZ
  * @param
  * @tparam
  * @note
  */
case class SaveParquetOperator(plugin: PhPluginTrait, name: String, args: PhWorkArgs[_]) extends PhOperatorTrait{
    override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        val prMapArgs = pr.toMapArgs[PhWorkArgs[_]]
        val defaultMapArgs = defaultArgs.toMapArgs[PhWorkArgs[_]]
        val inDFName = defaultMapArgs.getAs[PhStringArgs]("inDFName").get.get
        val path = defaultMapArgs.getAs[PhStringArgs]("path").get.get
        val inDF = prMapArgs.getAs[PhDFArgs](inDFName).get.get
        implicit val sd: PhSparkDriver = sparkObj
        sd.setUtil(save2Parquet()).save2Parquet(inDF, path)
        PhDFArgs(inDF)
    }
}
