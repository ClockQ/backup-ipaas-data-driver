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

import org.apache.spark.sql.{Column, DataFrame, functions}
import org.apache.spark.sql.functions.struct
import com.pharbers.ipaas.data.driver.api.work._

/** 利用GroupBy对数据集去重（稳定去重算法）
  *
  * @author clock
  * @version 0.1
  * @since 2019-05-28 17:21
  * @example 默认参数例子
  * {{{
  * inDFName: actionName // 要作用的 DataFrame 名字
  * keys: col_1#col_2 // 去重根据的列名，用`#`号分割
  * chooseBy: col_3 // 根据哪列进行去重（缺省值是数据集第一列）
  * chooseFun: max // 保留col_3中最大的一条（缺省值），或最小的一条（min）
  * }}}
  */
case class DistinctByKeyOperator(name: String,
                                 defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                                 pluginLst: Seq[PhPluginTrait2[Column]])
        extends PhOperatorTrait2[DataFrame] {
    /** 要作用的 DataFrame 名字 */
    val inDFName: String = defaultArgs.getAs[PhStringArgs]("inDFName").get.get
    /** 去重的根据列 */
    val keys: Array[String] = defaultArgs.getAs[PhStringArgs]("keys").get.get.split("#")
    /** 去重选择列 */
    val chooseBy: Option[PhStringArgs] = defaultArgs.getAs[PhStringArgs]("chooseBy")
    /** 去重选择函数 */
    val chooseFun: Option[PhStringArgs] = defaultArgs.getAs[PhStringArgs]("chooseFun")

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[DataFrame] = {
        val inDF = pr.getAs[PhDFArgs](inDFName).get.get
        val columns = inDF.columns
        val sortBy = chooseBy match {
            case Some(one) => one.get
            case None => columns.head
        }
        def sortFun(col: Column): Column = chooseFun match {
            case Some(str) =>
                str.get match {
                    case "min" => functions.min(col)
                    case _ => functions.max(col)
                }
            case None => functions.max(col)
        }

        val outDF = inDF.groupBy(keys.head, keys.tail: _*)
                .agg(sortFun(struct(sortBy, columns.filter(_ != sortBy): _*)) as "tmp")
                .select("tmp.*")
                .select(columns.head, columns.tail: _*)

        PhDFArgs(outDF)
    }
}
