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
import org.apache.spark.sql.functions.{col, concat, lit}
import com.pharbers.ipaas.data.driver.api.work.{PhColArgs, PhMapArgs, PhPluginTrait2, PhStringArgs, PhWorkArgs}

/** 按照指定分隔符拼接多列为字符串插件
  *
  * @author dcs
  * @version 0.1
  * @since 2019/6/12 18:25
  * @example 默认参数例子
  * {{{
  * columns: col_1#col_2 // 要拼接的多个列名，用`#`号分割
  * dilimiter: "," //拼接后字符串的分隔符
  * }}}
  */
case class ConcatStrPlugin(name: String,
                           defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                           subPluginLst: Seq[PhPluginTrait2[Column]])
        extends PhPluginTrait2[Column] {
    /** 要拼接的列名列表 */
    val columnList: Array[String] = defaultArgs.getAs[PhStringArgs]("columns").get.get.split("#")
    /** 要拼接的分隔符 */
    val dilimiter: String = defaultArgs.getAs[PhStringArgs]("dilimiter").get.get

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Column] = {
        val concatExpr = columnList.flatMap(x => Array(lit(dilimiter), col(x))).tail
        PhColArgs(concat(concatExpr: _*))
    }
}
