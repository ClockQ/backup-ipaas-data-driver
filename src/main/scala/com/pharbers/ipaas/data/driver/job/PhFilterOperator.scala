package com.pharbers.ipaas.data.driver.job

import com.pharbers.ipaas.data.driver.api.work._
import org.apache.spark.sql.functions._

/**
  * @author dcs
  * @param $args
  * @tparam T
  * @note
  */
case class PhFilterOperator(plugin: PhPluginTrait, name: String, args: PhWorkArgs[_]) extends PhOperatorTrait{
    override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        val tmp = pr match {
            case mapArgs: PhMapArgs[_] => mapArgs
            case _ => throw new Exception("参数类型错误")
        }

        val argsMap = args match {
            case mapArgs: PhMapArgs[_] => mapArgs
            case _ => throw new Exception("参数类型错误")
        }
        val inDF = tmp.get("df").asInstanceOf[PhDFArgs].get
        val resultDF = inDF.filter(expr(argsMap.get.getOrElse("filter", throw new Exception("无newColumnName配置")).asInstanceOf[PhStringArgs].get))
        PhDFArgs(resultDF)

    }
}
