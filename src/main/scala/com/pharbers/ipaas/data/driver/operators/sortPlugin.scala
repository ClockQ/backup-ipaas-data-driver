package com.pharbers.ipaas.data.driver.operators

import com.pharbers.ipaas.data.driver.api.work._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

case class sortPlugin() extends PhPluginTrait {
	override val name: String = "rankPlugin"
	override val defaultArgs: PhWorkArgs[_] = PhMapArgs(Map.empty)

	override def perform(args: PhWorkArgs[_]): PhWorkArgs[_] = {
		val argsMap = args.asInstanceOf[PhMapArgs].get
		val df = argsMap("df").asInstanceOf[PhDFArgs].get
		val sortList = argsMap("sortList").asInstanceOf[PhMapArgs].get.asInstanceOf[List[String]]
		val orderStr = argsMap("orderStr").asInstanceOf[PhStringArgs].get
		val descFunc: List[String] => List[Column] = lst => lst.map(x => -col(x))
		val ascFuc: List[String] => List[Column] = lst => lst.map(x => col(x))
		val funcMap = Map("asc" -> ascFuc, "desc" -> descFunc)
		val sortColList = funcMap(orderStr)(sortList)
		val resultDF = df.sort(sortColList: _*)
		PhDFArgs(resultDF)
	}
}
