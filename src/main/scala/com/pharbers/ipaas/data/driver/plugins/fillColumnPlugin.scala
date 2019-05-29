package com.pharbers.ipaas.data.driver.plugins

import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhMapArgs, PhNoneArgs, PhPluginTrait, PhStringArgs, PhWorkArgs}
import org.apache.spark.sql.functions._

case class fillColumnPlugin() extends PhPluginTrait {
	override val name: String = "fillColumnPlugin"
	override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

	override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
		val argsMap = pr.asInstanceOf[PhMapArgs[_]]
		val df = argsMap.getAs[PhDFArgs]("df").get.get
		val columnName = argsMap.getAs[PhStringArgs]("columnName").get.get
		val columnName1 = argsMap.getAs[PhStringArgs]("columnName1").get.get
		val replaceColumnName = argsMap.getAs[PhStringArgs]("replaceColumnName").get.get
		val defaultValue = argsMap.getAs[PhStringArgs]("defaultValue").get.get
		val resultDF = df.withColumn(columnName, when(col(columnName1).isNull, defaultValue).otherwise(col(replaceColumnName)))
		PhDFArgs(resultDF)
	}
}
