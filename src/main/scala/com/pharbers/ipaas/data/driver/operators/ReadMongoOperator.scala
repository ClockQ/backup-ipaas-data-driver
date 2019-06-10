package com.pharbers.ipaas.data.driver.operators

import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhMapArgs, PhNoneArgs, PhOperatorTrait, PhPluginTrait, PhStringArgs, PhWorkArgs}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.libs.spark.util._
import env.sparkObj

/** 功能描述
  *
  * @author EDZ
  * @param
  * @tparam
  * @note
  */
case class ReadMongoOperator(plugin: PhPluginTrait, name: String, args: PhWorkArgs[_]) extends PhOperatorTrait{
    override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        val defaultMapArgs = defaultArgs.toMapArgs[PhWorkArgs[_]]
        val mongodbHost = defaultMapArgs.getAs[PhStringArgs]("mongodbHost").get.get
        val mongodbPort = defaultMapArgs.getAs[PhStringArgs]("mongodbPort").get.get
        val databaseName = defaultMapArgs.getAs[PhStringArgs]("databaseName").get.get
        val collName = defaultMapArgs.getAs[PhStringArgs]("collName").get.get

        implicit val sd: PhSparkDriver = sparkObj
        PhDFArgs(sd.setUtil(readMongo()).readMongo(mongodbHost, mongodbPort, databaseName, collName))
    }
}