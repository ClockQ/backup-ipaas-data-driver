package com.pharbers.ipaas.data.driver.job

import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.libs.spark.util.readParquet
import env.sparkObj

/**
  * @author dcs
  * @param $args
  * @tparam T
  * @note
  */
case class PhReadParquetOperator(name: String, args: PhWorkArgs[_], plugin: PhPluginTrait) extends PhOperatorTrait{
    override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        val tmp = args match {
            case mapArgs: PhMapArgs[_] => mapArgs
            case _ => throw new Exception("参数类型错误")
        }
        implicit val sd: PhSparkDriver = sparkObj
        PhDFArgs(sd.setUtil(readParquet()).readParquet(tmp.get.getOrElse("path", throw new Exception("配置文件中没有path配置")).asInstanceOf[PhStringArgs].get))
    }
}