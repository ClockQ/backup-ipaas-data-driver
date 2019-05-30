package com.pharbers.ipaas.data.driver.plugins

import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhMapArgs, PhNoneArgs, PhPluginTrait, PhStringArgs, PhWorkArgs}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}

case class SplitPlugin() extends PhPluginTrait{
	override val name: String = "splitPlugin"
	override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

	override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
		val prMapArgs = pr.asInstanceOf[PhMapArgs[_]]
		val df = prMapArgs.getAs[PhDFArgs]("df").get.get
		val splitedColName = prMapArgs.getAs[PhStringArgs]("splitedColName").get.get
		val formatFunc: UserDefinedFunction = udf { lst: Seq[String] => lst.dropRight(1).mkString(" ")}
		val resultDF = df.withColumn(splitedColName, formatFunc(col(splitedColName)))
		PhDFArgs(resultDF)
	}
}
