package com.pharbers.ipaas.data.driver.operators

import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhNoneArgs, PhOperatorTrait, PhPluginTrait, PhStringArgs, PhWorkArgs}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.libs.spark.util.{save2Mongo, save2Parquet}
import env.sparkObj

/** 功能描述
  *
  * @author EDZ
  * @param
  * @tparam
  * @note
  */
case class SaveMongoOperator(plugin: PhPluginTrait, name: String, args: PhWorkArgs[_]) extends PhOperatorTrait{
    override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        val prMapArgs = pr.toMapArgs[PhWorkArgs[_]]
        val defaultMapArgs = defaultArgs.toMapArgs[PhWorkArgs[_]]
        val inDFName = defaultMapArgs.getAs[PhStringArgs]("inDFName").get.get
        val inDF = prMapArgs.getAs[PhDFArgs](inDFName).get.get
        val mongodbHost = defaultMapArgs.getAs[PhStringArgs]("mongodbHost").get.get
        val mongodbPort = defaultMapArgs.getAs[PhStringArgs]("mongodbPort").get.get
        val databaseName = defaultMapArgs.getAs[PhStringArgs]("databaseName").get.get
        val collName = defaultMapArgs.getAs[PhStringArgs]("collName").get.get

        implicit val sd: PhSparkDriver = sparkObj
        sd.setUtil(save2Mongo()).save2Mongo(inDF, mongodbHost, mongodbPort, databaseName, collName)
        PhDFArgs(inDF)
    }
}