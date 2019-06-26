package com.pharbers.ipaas.data.driver.operators

import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhMapArgs, PhOperatorTrait, PhPluginTrait, PhStringArgs, PhWorkArgs}
import org.apache.spark.sql.{Column, DataFrame}

/** 用null值补充inDF的列，使其列不少于moreColDF
  *
  * @author cui
  * @version 0.1
  * @since 2019/6/15 18:10
  * @example 默认参数例子
  * {{{
  *     inDFName: String // 要新增列的DataFrame
  *     moreColDFName: String // 目标多列 DataFrame 名字
  * }}}
  */

case class AddDiffColsOperator(name: String,
                               defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                               pluginLst: Seq[PhPluginTrait[Column]])
	extends PhOperatorTrait[DataFrame] {
	/** 目标多列 DataFrame 名字 */
	val moreColDFName: String = defaultArgs.getAs[PhStringArgs]("moreColDFName").get.get
	/** 要新增列的DataFrame */
	val inDFName: String = defaultArgs.getAs[PhStringArgs]("inDFName").get.get
	override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[DataFrame] = {
		val prMapArgs = pr.toMapArgs[PhWorkArgs[_]]
		val inDF = prMapArgs.getAs[PhDFArgs](inDFName).get.get
		val moreColDF = prMapArgs.getAs[PhDFArgs](moreColDFName).get.get
		val addColList = moreColDF.columns.diff(inDF.columns)
		val outDF = addColList.foldRight(inDF)((a, b) => b.withColumn(a, null))
		PhDFArgs(outDF)
	}
}
