package com.pharbers.ipaas.data.driver.plugins

import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhMapArgs, PhNoneArgs, PhPluginTrait, PhWorkArgs}

case class FullJoinPlugin() extends PhPluginTrait {
	override val name: String = "fullJoinPlugin"
	override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

	override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
		val argsMap = pr.asInstanceOf[PhMapArgs[_]]
		val leftDF = argsMap.getAs[PhDFArgs]("leftDF").get.get
		val rightDF = argsMap.getAs[PhDFArgs]("rightDF").get.get
		val resultDF = leftDF.distinct().join(rightDF.distinct())
		PhDFArgs(resultDF)
	}
}
