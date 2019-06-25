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

import com.pharbers.ipaas.data.driver.api.work._
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

/** 计算EI（今年的share/去年同期的share）
  *
  * @author dcs
  * @version 0.1
  * @since 2019/6/24 15:16
  * @example 默认参数例子
  * {{{
  *     valueColumnName: String 值所在列名
  *     dateColName: String 日期所在列名
  *     partitionColumnNames: List[String] 需要分组列的集合
  * }}}
  */
case class CalcEI(name: String,
                  defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                  subPluginLst: Seq[PhPluginTrait[Column]])
	extends PhPluginTrait[Column] {
//	值所在列名
	val valueColumnName: String = defaultArgs.getAs[PhStringArgs]("valueColumnName").get.get
//	日期所在列名
	val dateColName: String = defaultArgs.getAs[PhStringArgs]("dateColName").get.get
//	需要分组列的集合
	val partitionColumnNames: List[String] = defaultArgs.getAs[PhListArgs[String]]("partitionColumnNames").get.get
    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Column] = {
        val windowYearOnYear = Window.partitionBy(partitionColumnNames.map(x => col(x)): _*).orderBy(col(dateColName).cast(IntegerType)).rangeBetween(-100, -100)
        PhColArgs(col(valueColumnName) / first(col(valueColumnName)).over(windowYearOnYear))
    }
}