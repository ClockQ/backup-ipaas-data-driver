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

import com.pharbers.ipaas.data.driver.api.work.{PhColArgs, PhMapArgs, PhPluginTrait, PhStringArgs, PhWorkArgs}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, explode, split}

/** 功能描述
  * 按一个column内容拆分一行为多行
  * @author EDZ
  * @version 0.0
  * @since 2019/06/25 10:53
  * @note
  * {{{
  * splitColName: MONTH // 需要拆分的column
  * delimit: 、 // 按这个字符串分割
  * }}}
  */
case class SplitColumnPlugin(name: String,
                        defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                        subPluginLst: Seq[PhPluginTrait[Column]])
        extends PhPluginTrait[Column] {

    val argsMap: PhMapArgs[_] = defaultArgs.asInstanceOf[PhMapArgs[_]]
    //需要拆分的column
    val splitColName: String = argsMap.getAs[PhStringArgs]("splitColName").get.get
    //按这个字符串分割
    val delimit: String = argsMap.getAs[PhStringArgs]("delimit").get.get

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Column] = {
        PhColArgs(explode(split(col(splitColName), delimit)))
    }
}
