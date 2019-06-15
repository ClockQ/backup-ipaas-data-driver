package com.pharbers.ipaas.data.driver.operators

import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhMapArgs, PhNoneArgs, PhOperatorTrait, PhPluginTrait, PhStringArgs, PhWorkArgs}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.libs.spark.util.readCsv
import env.sparkObj

/**
  * @author dcs
  * @param $args
  * @tparam T
  * @note
  */
case class PhReadOperator(plugin: PhPluginTrait, name: String, args: PhWorkArgs[_]) extends PhOperatorTrait{
    override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        val tmp = args match {
            case mapArgs: PhMapArgs[_] => mapArgs
            case _ => throw new Exception("参数类型错误")
        }
        implicit val sd: PhSparkDriver = sparkObj
        PhDFArgs(sd.setUtil(readCsv()).readCsv(tmp.get.getOrElse("path", throw new Exception("配置文件中没有path配置")).asInstanceOf[PhStringArgs].get))
    }
}
