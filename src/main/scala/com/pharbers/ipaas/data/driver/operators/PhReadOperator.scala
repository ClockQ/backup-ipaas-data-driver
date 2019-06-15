package com.pharbers.ipaas.data.driver.operators

import org.apache.spark.sql.{Column, DataFrame}
import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.libs.spark.util.readCsv
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver

case class PhReadOperator(name: String,
                          defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                          pluginLst: Seq[PhPluginTrait2[Column]])
        extends PhOperatorTrait2[DataFrame] {

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[DataFrame] = {
        implicit val sd: PhSparkDriver = pr.get("sparkDriver").asInstanceOf[PhSparkDriverArgs].get

        PhDFArgs(sparkDriver.setUtil(readCsv()).readCsv(tmp.get.getOrElse("path", throw new Exception("配置文件中没有path配置")).asInstanceOf[PhStringArgs].get))
    }
}
