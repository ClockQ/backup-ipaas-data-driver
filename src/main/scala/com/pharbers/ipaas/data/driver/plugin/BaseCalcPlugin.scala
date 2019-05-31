package com.pharbers.ipaas.data.driver.plugin

import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhMapArgs, PhNoneArgs, PhPluginTrait, PhStringArgs, PhWorkArgs}
import org.apache.spark.sql.functions.expr

/** 这个类是干啥的
  *
  * @author
  * @param args 参数说明
  * @tparam T 类型参数说明
  * @note 一些值得注意的地方
  */
case class BaseCalcPlugin() extends PhPluginTrait {
	override val name: String = "CalcPlugin"
	override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

	override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
		val argsMap = pr.asInstanceOf[PhMapArgs[_]]
		val df = argsMap.getAs[PhDFArgs]("df").get.get
		val columnNameNew = argsMap.getAs[PhStringArgs]("columnNameNew").get.get
		val exprString = argsMap.getAs[PhStringArgs]("exprString").get.get
		val resultDF = df.withColumn(columnNameNew, expr(exprString))
		PhDFArgs(resultDF)
	}
}
