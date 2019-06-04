package com.pharbers.ipaas.data.driver.job

import com.pharbers.ipaas.data.driver.api.work._
import org.apache.spark.sql.DataFrame

/**
  * @author dcs
  * @param $args
  * @tparam T
  * @note
  */
case class PhBaseOperator(plugin: PhPluginTrait, name: String, args: PhWorkArgs[_]) extends PhOperatorTrait{
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
        val colFun = plugin.perform(argsMap).toColArgs.get
        PhDFArgs(tmp.get("df").asInstanceOf[PhDFArgs].get.withColumn(args.toMapArgs[PhStringArgs]
                .get.getOrElse("newColumnName", throw new Exception("无newColumnName配置")).get, colFun))
    }
}
