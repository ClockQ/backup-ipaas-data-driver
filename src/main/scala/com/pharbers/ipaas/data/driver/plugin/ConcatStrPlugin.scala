package com.pharbers.ipaas.data.driver.plugin

import com.pharbers.ipaas.data.driver.api.work.{PhColArgs, PhDFArgs, PhListArgs, PhMapArgs, PhNoneArgs, PhPluginTrait, PhStringArgs, PhWorkArgs}
import org.apache.spark.sql.functions.{col, concat, lit}

/** 这个类是干啥的
  *
  * @author
  * @param args 参数说明
  * @tparam T 类型参数说明
  * @note 一些值得注意的地方
  */
case class ConcatStrPlugin(name: String, defaultArgs: PhWorkArgs[_]) extends PhPluginTrait {

	override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
		val argsMap = pr.asInstanceOf[PhMapArgs[_]]
		val columnList = argsMap.getAs[PhListArgs[PhStringArgs]]("columnList").get.get.map(x => col(x.get))
		val dilimiter = argsMap.getAs[PhStringArgs]("dilimiter").get.get
		val concatExpr = columnList.head :: columnList.tail.flatMap(x => List(lit(dilimiter), x))
		PhColArgs(concat(concatExpr: _*))
	}
}
