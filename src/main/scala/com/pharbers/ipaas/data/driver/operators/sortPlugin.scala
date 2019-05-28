package com.pharbers.ipaas.data.driver.operators

import com.pharbers.ipaas.data.driver.api.work._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.col

case class sortPlugin() extends PhPluginTrait {
	override val name: String = "rankPlugin"
	override val defaultArgs: PhWorkArgs[_] = PhMapArgs(Map.empty)

	override def perform(args: PhWorkArgs[Any]): PhWorkArgs[_] = {
		val argsMap = args.asInstanceOf[PhMapArgs[_]]
		val df = argsMap.getAs[PhDFArgs]("df").get
		val sortList = argsMap.getAs[PhListArgs[PhStringArgs]]("sortList").get.map(x => x.get)
		val orderStr = argsMap.getAs[PhStringArgs]("orderStr").get
		val descFunc: List[String] => List[Column] = lst => lst.map(x => -col(x))
		val ascFuc: List[String] => List[Column] = lst => lst.map(x => col(x))
		val funcMap = Map("asc" -> ascFuc, "desc" -> descFunc)
		val sortColList = funcMap(orderStr)(sortList)
		val resultDF = df.sort(sortColList: _*)
		df.sort()
		PhDFArgs(resultDF)
	}
}
