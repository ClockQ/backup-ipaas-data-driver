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

import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{Column, DataFrame}
import com.pharbers.ipaas.data.driver.api.work._

/** 使用 Expr 表达式过滤 DataFrame
 *
 * @author clock
 * @version 0.1
 * @since 2019/6/11 16:59
 * @example 默认参数例子
 * {{{
 * inDFName: actionName // 要作用的 DataFrame 名字
 * filter: DATE == 201801 // 过滤的 Expr 表达式
 * }}}
 */
case class ExprFilterOperator(name: String,
                              defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                              pluginLst: Seq[PhPluginTrait[Column]])(implicit ctx: PhMapArgs[PhWorkArgs[_]])
        extends PhOperatorTrait[DataFrame] {
    /** 要作用的 DataFrame 名字 */
    val inDFName: String = defaultArgs.getAs[PhStringArgs]("inDFName").get.get
    /** filter expr 表达式 */
    val exprFilter: String = defaultArgs.getAs[PhStringArgs]("filter").get.get
    val replaceString: Map[String, String] = defaultArgs.get.map(x => (x._1, x._2.get.toString))

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[DataFrame] = {
        val exprStr = replaceString.foldLeft(exprFilter)((l, r) => {
            l.replace(s"#${r._1}#", r._2)
        })
        val inDF = pr.getAs[PhDFArgs](inDFName).get.get
        val outDF = inDF.filter(expr(exprStr))
        PhDFArgs(outDF)
    }
}
