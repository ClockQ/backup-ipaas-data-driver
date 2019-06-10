package com.pharbers.ipaas.data.driver.operators

import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhOperatorTrait, PhPluginTrait, PhStringArgs, PhWorkArgs}

case class cacaheOperator(plugin: PhPluginTrait, name: String, defaultArgs: PhWorkArgs[_]) extends PhOperatorTrait {

	override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
		val prMapArgs = pr.toMapArgs[PhWorkArgs[_]]
		val inDF = prMapArgs.getAs[PhDFArgs]("inDF").get.get
		val outDF = inDF.cache()
		PhDFArgs(outDF)
	}
}