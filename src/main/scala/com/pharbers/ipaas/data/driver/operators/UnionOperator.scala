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

package com.pharbers.ipaas.data.driver.operators

import com.pharbers.ipaas.data.driver.api.work._
import org.apache.spark.sql.{Column, DataFrame}

/** 连接两个数据集
  *
  * @author clock
  * @version 0.1
  * @since 2019/6/18 17:00
  * @example 默认参数例子
  * {{{
  *  inDFName: actionName // 要作用的 DataFrame 名字
  *  unionDFName: unionDFName // 连接的 DataFrame 名字
  * }}}
  */
case class UnionOperator(name: String,
                         defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                         pluginLst: Seq[PhPluginTrait2[Column]]) extends PhOperatorTrait2[DataFrame] {
    /**要作用的 DataFrame 名字*/
    val inDFName: String = defaultArgs.getAs[PhStringArgs]("inDFName").get.get
    /**连接的 DataFrame 名字*/
    val unionDFName: String = defaultArgs.getAs[PhStringArgs]("unionDFName").get.get

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[DataFrame] = {
        val prMapArgs = pr.toMapArgs[PhWorkArgs[_]]
        val inDF = prMapArgs.getAs[PhDFArgs](inDFName).get.get
        val unionDF = prMapArgs.getAs[PhDFArgs](unionDFName).get.get
        val outDF = inDF.unionByName(unionDF)

        PhDFArgs(outDF)
    }
}
