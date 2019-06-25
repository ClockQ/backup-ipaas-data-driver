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

import com.pharbers.ipaas.data.driver.api.work.{PhColArgs, PhListArgs, PhMapArgs, PhNoneArgs, PhPluginTrait, PhStringArgs, PhWorkArgs}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

/** 功能描述
  *
  * @version 0.1
  * @since 2019/6/24 15:16
  * @example
  * {{{
  *      oidColName: String oid所在列名
  * }}}
  */
case class Oid2Id(name: String,
                  defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                  subPluginLst: Seq[PhPluginTrait[Column]])
	extends PhPluginTrait[Column] {

	val oidColName: String = defaultArgs.getAs[String]("oidColName").get

	override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Column] = {
        PhColArgs(lit(col(oidColName)("oid")))
    }
}