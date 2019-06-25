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

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.expr
import com.pharbers.ipaas.data.driver.api.work._

/** 利用GroupBy对数据集去重（稳定去重算法）
  *
  * @author clock
  * @version 0.1
  * @since 2019-05-28 17:21
  * @example 默认参数例子
  * {{{
  * inDFName: actionName // 要作用的 DataFrame 名字
  * groups: col_1#col_2 // group 的列集合，用`#`号分割
  * aggExprs: sum(UNITS) as UNITS // group 的 聚合操作集合，用`#`号分割
  * }}}
  */
case class GroupOperator(name: String,
                         defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                         pluginLst: Seq[PhPluginTrait[Column]])
        extends PhOperatorTrait[DataFrame] {
    /** 要作用的 DataFrame 名字 */
    val inDFName: String = defaultArgs.getAs[PhStringArgs]("inDFName").get.get
    /** group 的列集合，用`#`号分割 */
    val groups: Array[String] = defaultArgs.getAs[PhStringArgs]("groups").get.get.split("#")
    /** group 的 聚合操作集合，用`#`号分割 */
    val aggExprs: Array[Column] = defaultArgs.getAs[PhStringArgs]("aggExprs").get.get.split("#").map(x => expr(x))

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[DataFrame] = {
        val inDF = pr.getAs[PhDFArgs](inDFName).get.get
        val outDF = inDF.groupBy(groups.head, groups.tail: _*).agg(aggExprs.head ,aggExprs.tail: _*)
        PhDFArgs(outDF)
    }
}