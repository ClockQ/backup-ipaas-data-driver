package com.pharbers.ipaas.data.driver.operators

import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhMapArgs, PhNoneArgs, PhOperatorTrait, PhPluginTrait, PhStringArgs, PhWorkArgs}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.libs.spark.util.{save2Mongo, save2Parquet}
import env.sparkObj

/** 功能描述
  * 存mongo算子
  * @param plugin 插件
  * @param name 算子 name
  * @param defaultArgs 配置参数 "inDFName"-> pr中的df名, "mongodbHost"-> 地址, "mongodbPort" -> 端口, "databaseName" -> 库名, "collName" -> 表名
  * @author dcs
  * @version 0.0
  * @since 2019/6/11 16:50
  * @note 一些值得注意的地方
  */
case class SaveMongoOperator(plugin: PhPluginTrait, name: String, defaultArgs: PhWorkArgs[_]) extends PhOperatorTrait{
    val defaultMapArgs: PhMapArgs[PhWorkArgs[_]] = defaultArgs.toMapArgs[PhWorkArgs[_]]
    val inDFName: String = defaultMapArgs.getAs[PhStringArgs]("inDFName").get.get
    val mongodbHost: String = defaultMapArgs.getAs[PhStringArgs]("mongodbHost").get.get
    val mongodbPort: String = defaultMapArgs.getAs[PhStringArgs]("mongodbPort").get.get
    val databaseName: String = defaultMapArgs.getAs[PhStringArgs]("databaseName").get.get
    val collName: String = defaultMapArgs.getAs[PhStringArgs]("collName").get.get

    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        val prMapArgs = pr.toMapArgs[PhWorkArgs[_]]
        val inDF = prMapArgs.getAs[PhDFArgs](inDFName).get.get

        implicit val sd: PhSparkDriver = sparkObj
        sd.setUtil(save2Mongo()).save2Mongo(inDF, mongodbHost, mongodbPort, databaseName, collName)
        PhDFArgs(inDF)
    }
}