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

import com.pharbers.ipaas.data.driver.api.work.{PhColArgs, PhListArgs, PhMapArgs, PhNoneArgs, PhOperatorTrait, PhPluginTrait, PhStringArgs, PhWorkArgs}
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** 计算占比
  * 环比 = （当月 - 上月） / 上月
  *
  * @author dcs
  * @version 0.1
  * @since 2019/6/24 15:16
  * @example df.CalcShare("$name", CalcMat().CalcRankByWindow(PhMapArgs).get)
  *          {{{
  *                valueColumnName: String 值所在列名
  *                partitionColumnNames: List[String] 需要分组列的集合
  *          }}}
  */

case class CalcShare(name: String,
                     defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                     subPluginLst: Seq[PhPluginTrait[Column]])
	extends PhPluginTrait[Column] {
	//	值所在列名
	val valueColumnName: String = defaultArgs.getAs[PhStringArgs]("valueColumnName").get.get
	//	需要分组列的集合
	val partitionColumnNames: List[String] = defaultArgs.getAs[PhListArgs[String]]("partitionColumnNames").get.get

	override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Column] = {
		val windowYearOnYear = Window.partitionBy(partitionColumnNames.map(x => col(x)): _*).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
		PhColArgs(col(valueColumnName) / sum(col(valueColumnName)).over(windowYearOnYear))
	}
}
