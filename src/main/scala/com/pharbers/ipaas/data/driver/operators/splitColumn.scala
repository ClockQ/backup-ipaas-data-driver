//package com.pharbers.ipaas.data.driver.operators
//
//import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhFuncArgs, PhMapArgs, PhNoneArgs, PhOperatorTrait, PhStringArgs, PhWorkArgs}
//import org.apache.spark.sql.expressions.UserDefinedFunction
//import org.apache.spark.sql.functions.{col, udf}
//
//case class splitColumn() extends PhOperatorTrait{
//	override val name: String = "splitColumn"
//	override val defaultArgs: PhWorkArgs[_] = PhNoneArgs
//
//	override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
//		val prMapArgs = pr.toMapArgs[PhWorkArgs[_]]
//		val df = prMapArgs.getAs[PhDFArgs]("df").get.get
//		val delimiter = prMapArgs.getAs[PhStringArgs]("delimiter").get.get
//		val splitedColName = prMapArgs.getAs[PhStringArgs]("splitedColName").get.get
//		val splitColName = prMapArgs.getAs[PhStringArgs]("splitColName").get.get
//		val splitFunc: UserDefinedFunction = udf { str: String => str.split(delimiter)}
//		val splitedDF = df.withColumn(splitedColName, splitFunc(col(splitColName)))
//		val replaceMap = PhMapArgs(prMapArgs.get ++ Map("df" -> PhDFArgs(splitedDF)))
//		val pluginResultDF = prMapArgs.getAs[PhFuncArgs[Any, Any]]("plugin").get.get(replaceMap).asInstanceOf[PhDFArgs]
//		pluginResultDF
//	}
//}
