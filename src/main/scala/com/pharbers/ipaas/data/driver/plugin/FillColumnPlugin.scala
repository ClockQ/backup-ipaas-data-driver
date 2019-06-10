package com.pharbers.ipaas.data.driver.plugin

import com.pharbers.ipaas.data.driver.api.work._
import org.apache.spark.sql.functions._

/** 这个类是干啥的
  *
  * @author
  * @param args 参数说明
  * @tparam T 类型参数说明
  * @note 一些值得注意的地方
  */
case class FillColumnPlugin(name: String, defaultArgs: PhWorkArgs[_]) extends PhPluginTrait {
	override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
		val argsMap = defaultArgs.asInstanceOf[PhMapArgs[_]]
		val condition = argsMap.getAs[PhStringArgs]("condition").get.get
		val replaceColumnName = argsMap.getAs[PhStringArgs]("replaceColumnName").get.get
		val defaultValue = argsMap.getAs[PhStringArgs]("defaultValue").get.get
		val colResult = when(expr(condition), defaultValue).otherwise(col(replaceColumnName))
		PhColArgs(colResult)
	}
}
