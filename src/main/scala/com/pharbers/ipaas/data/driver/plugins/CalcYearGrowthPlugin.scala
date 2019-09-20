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

import com.pharbers.ipaas.data.driver.api.Annotation.Plugin
import com.pharbers.ipaas.data.driver.api.work._
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, first}
import org.apache.spark.sql.types.IntegerType

/** 计算同比
 *
 * @author dcs
 * @version 0.1
 * @since 2019/6/24 15:16
 * @note 同比 =（当月 - 去年当月） / 去年当月
 * @example df.CalcYearGrowth(col("name"), CalcMat().CalcRankByWindow(PhMapArgs).get)
 * {{{
 *       valueColumnName: String 值所在列名
 *       dateColName: String 日期所在列名
 *       partitionColumnNames: String 需要分组列的集合，使用"#"分隔
 * }}}
 */
@Plugin(name = "calc_year_growth", args = Array("valueColumnName", "dateColName", "partitionColumnNames"), msg = "calc year growth")
case class CalcYearGrowthPlugin(name: String,
                                defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                                subPluginLst: Seq[PhPluginTrait[Column]])(implicit ctx: PhMapArgs[PhWorkArgs[_]])
        extends PhPluginTrait[Column] {
    /** 值所在列名 */
    val valueColumnName: String = defaultArgs.getAs[PhStringArgs]("valueColumnName").get.get
    /** 日期所在列名 */
    val dateColName: String = defaultArgs.getAs[PhStringArgs]("dateColName").get.get
    /** 需要分组列的集合，使用"#"分隔 */
    val partitionColumnNames: List[String] = defaultArgs.getAs[PhStringArgs]("partitionColumnNames").get.get.split("#").toList

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Column] = {
        val windowYearOnYear = Window.partitionBy(partitionColumnNames.map(x => col(x)): _*).orderBy(col(dateColName).cast(IntegerType)).rangeBetween(-100, -100)
        PhColArgs((col(valueColumnName) - first(col(valueColumnName)).over(windowYearOnYear)) / first(col(valueColumnName)).over(windowYearOnYear))
    }
}
