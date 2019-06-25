/*
 * This file is part of com.pharbers.ipaas-data-driver.
 *
 * com.pharbers.ipaas-data-driver is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * com.pharbers.ipaas-data-driver is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.pharbers.ipaas.data.driver.plugins

import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhListArgs, PhMapArgs, PhNoneArgs, PhPluginTrait, PhStringArgs, PhWorkArgs}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.col

/** 按照某一列排序
  *
  * @author cui
  * @version 0.1
  * @since 2019/6/24 15:16
  * @example 默认参数例子
  * {{{
  *     valueColumnName: String 值所在列名
  *     dateColName: String 日期所在列名
  *     partitionColumnNames: List[String] 需要分组列的集合
  * }}}
  */
case class SortPlugin(name: String,
                      defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                      subPluginLst: Seq[PhPluginTrait[Column]])
	extends PhPluginTrait[DataFrame] {
	val inDF: DataFrame = defaultArgs.getAs[PhDFArgs]("inDF").get.get
	val orderStr: String = defaultArgs.getAs[PhStringArgs]("orderStr").get.get
	val sortList: List[String] = defaultArgs.getAs[PhListArgs[String]]("sortList").get.get

	override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[DataFrame] = {
		val descFunc: List[String] => List[Column] = lst => lst.map(x => -col(x))
		val ascFuc: List[String] => List[Column] = lst => lst.map(x => col(x))
		val funcMap = Map("asc" -> ascFuc, "desc" -> descFunc)
		val sortColList = funcMap(orderStr)(sortList)
		val resultDF = inDF.sort(sortColList: _*)
		PhDFArgs(resultDF)
	}
}
