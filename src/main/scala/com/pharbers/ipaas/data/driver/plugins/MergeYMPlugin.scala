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
import org.apache.spark.sql.functions._

/** 将年月两列拼接成六位长度的YM（比如把year=2018,month=1或01的数据拼接为201801）
  *
  * @author clock
  * @version 0.1
  * @since 2019/6/17 18:15
  * @example 默认参数例子
  * {{{
  * yearColName: YEAR // 年列名
  * monthColName: MONTH // 月列名
  * }}}
  */
case class MergeYMPlugin(name: String,
                         defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                         subPluginLst: Seq[PhPluginTrait[Column]])
        extends PhPluginTrait[Column] {
    /** 年列名 */
    val yearColName: String = defaultArgs.getAs[PhStringArgs]("yearColName").get.get
    /** 月列名 */
    val monthColName: String = defaultArgs.getAs[PhStringArgs]("monthColName").get.get

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Column] = {
        PhColArgs(
            when(
                col(monthColName).>=(10), concat(col(yearColName), col(monthColName))
            ).otherwise(
                concat(col(yearColName), lit("0"), col(monthColName))
            )
        )
    }
}