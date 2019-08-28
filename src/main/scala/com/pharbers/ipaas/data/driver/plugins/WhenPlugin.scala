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

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import com.pharbers.ipaas.data.driver.api.work._

/** 按照指定分隔符拼接多列为字符串插件
 *
 * @author dcs
 * @version 0.1
 * @since 2019/6/12 18:25
 * @example 默认参数例子
 * {{{
 *         condition: "VALUE % 2 == 1" // when 条件
 *         value: "VALUE + 1" // 符合条件的结果
 * }}}
 */
case class WhenPlugin(name: String,
                      defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                      subPluginLst: Seq[PhPluginTrait[Column]])(implicit ctx: PhMapArgs[PhWorkArgs[_]])
        extends PhPluginTrait[Column] {

    /** when 条件 */
    val condition: String = defaultArgs.getAs[PhStringArgs]("condition").get.get
    /** 符合条件的结果 */
    val value: String = defaultArgs.getAs[PhStringArgs]("value").get.get

    def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Column] = {
        val func = subPluginLst.headOption match {
            case Some(sub) => sub.perform(pr).get
            case _ => lit("")
        }
        PhColArgs(when(expr(condition), expr(value)).otherwise(func))
    }
}
