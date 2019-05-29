package com.pharbers.ipaas.data.driver.plugins

import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhMapArgs, PhNoneArgs, PhPluginTrait, PhStringArgs, PhWorkArgs}
import org.apache.spark.sql.functions._

case class CalcPlugin() extends PhPluginTrait {
	override val name: String = "fillColumnPlugin"
	override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

	override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
		val argsMap = pr.asInstanceOf[PhMapArgs[_]]
		val df = argsMap.getAs[PhDFArgs]("df").get.get
		val columnName = argsMap.getAs[PhStringArgs]("columnName").get.get
		val columnName1 = argsMap.getAs[PhStringArgs]("columnName1").get.get
		val columnName2 = argsMap.getAs[PhStringArgs]("columnName2").get.get
		val defaultValue = argsMap.getAs[PhStringArgs]("defaultValue").get.get
		df.withColumn(columnName, when(col(columnName1).isNull, defaultValue).otherwise(col(columnName2)))
		val resultDF = df
		PhDFArgs(resultDF)
	}
}
