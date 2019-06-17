package com.pharbers.ipaas.data.driver.plugins

import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhListArgs, PhMapArgs, PhNoneArgs, PhPluginTrait, PhStringArgs, PhWorkArgs}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

/** 这个类是干啥的
  *
  * @author
  * @param args 参数说明
  * @tparam T 类型参数说明
  * @note 一些值得注意的地方
  */
case class SortPlugin() extends PhPluginTrait {
	override val name: String = "sortPlugin"
	override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

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
		PhDFArgs(resultDF)
	}
}
