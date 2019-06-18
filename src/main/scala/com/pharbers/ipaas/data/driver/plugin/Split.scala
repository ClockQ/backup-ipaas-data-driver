package com.pharbers.ipaas.data.driver.plugin

import com.pharbers.ipaas.data.driver.api.work.{PhColArgs, PhMapArgs, PhNoneArgs, PhPluginTrait, PhStringArgs, PhWorkArgs}
import org.apache.spark.sql.functions._

/** 功能描述
  * 拆分一行为多行
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author EDZ
  * @version 0.0
  * @since 2019/06/13 15:55
  * @note 一些值得注意的地方
  */
case class Split(name: String = "Split", defaultArgs: PhWorkArgs[_] = PhNoneArgs) extends PhPluginTrait {
    val argsMap: PhMapArgs[_] = defaultArgs.asInstanceOf[PhMapArgs[_]]
    val splitColName: String = argsMap.getAs[PhStringArgs]("splitColName").get.get
    val delimit: String = argsMap.getAs[PhStringArgs]("delimit").get.get

    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {

        PhColArgs(explode(split(col(splitColName), delimit)))
    }
}