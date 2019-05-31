package com.pharbers.ipaas.data.driver.plugin

import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhMapArgs, PhNoneArgs, PhPluginTrait, PhStringArgs, PhWorkArgs}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}

/** 这个类是干啥的
  *
  * @author dcs
  * @param args 参数说明
  * @tparam T 类型参数说明
  * @note 一些值得注意的地方
  */
case class SplitTakeHeadPlugin() extends PhPluginTrait{
	override val name: String = "splitTakeHeadPlugin"
	override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

	override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
		val prMapArgs = pr.asInstanceOf[PhMapArgs[_]]
		val df = prMapArgs.getAs[PhDFArgs]("df").get.get
		val splitedColName = prMapArgs.getAs[PhStringArgs]("splitedColName").get.get
		val formatFunc: UserDefinedFunction = udf { lst: Seq[String] => lst.head}
		val resultDF = df.withColumn(splitedColName, formatFunc(col(splitedColName)))
		PhDFArgs(resultDF)
	}
}
