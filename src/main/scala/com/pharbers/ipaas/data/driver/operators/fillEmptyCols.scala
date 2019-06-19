//package com.pharbers.ipaas.data.driver.operators
//
//import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhOperatorTrait, PhPluginTrait, PhStringArgs, PhWorkArgs}
//
//case class fillEmptyCols(plugin: PhPluginTrait, name: String, defaultArgs: PhWorkArgs[_]) extends PhOperatorTrait {
//	override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
//		val prMapArgs = pr.toMapArgs[PhWorkArgs[_]]
//		val moreColDFName = prMapArgs.getAs[PhStringArgs]("moreColDFName").get.get
//		val inDF = prMapArgs.getAs[PhDFArgs]("inDF").get.get
//		val moreColDF = prMapArgs.getAs[PhDFArgs](moreColDFName).get.get
//		val addColList = moreColDF.columns.diff(inDF.columns)
//		val outDF = addColList.foldRight(inDF)((a, b) => b.withColumn(a, null))
//		PhDFArgs(outDF)
//	}
//}
