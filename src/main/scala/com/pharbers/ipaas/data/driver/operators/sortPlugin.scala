package com.pharbers.ipaas.data.driver.operators

import com.pharbers.ipaas.data.driver.api.work._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.col

case class sortPlugin() extends PhPluginTrait2 {
	override val name: String = "rankPlugin"
	override val defaultArgs: PhWorkArgs[_] = PhMapArgs(Map.empty)

	override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
		val argsMap = pr.asInstanceOf[PhMapArgs[_]]
		val df = argsMap.getAs[PhDFArgs]("df").get.get
		val sortList = argsMap.getAs[PhListArgs[PhStringArgs]]("sortList").get.get.map(x => x.get)
		val orderStr = argsMap.getAs[PhStringArgs]("orderStr").get.get
		val descFunc: List[String] => List[Column] = lst => lst.map(x => -col(x))
		val ascFuc: List[String] => List[Column] = lst => lst.map(x => col(x))
		val funcMap = Map("asc" -> ascFuc, "desc" -> descFunc)
		val sortColList = funcMap(orderStr)(sortList)
		val resultDF = df.sort(sortColList: _*)
		df.sort()
		PhDFArgs(resultDF)
	}
}
