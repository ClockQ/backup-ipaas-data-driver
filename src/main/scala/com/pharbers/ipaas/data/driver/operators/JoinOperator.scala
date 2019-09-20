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

import com.pharbers.ipaas.data.driver.api.Annotation.Operator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import com.pharbers.ipaas.data.driver.api.work._

/** JOIN 算子
 *
 * @author clock
 * @version 0.1
 * @since 2019/6/11 16:50
 * @example 默认参数例子
 * {{{
 * inDFName: actionName // 要作用的 DataFrame 名字
 * joinDFName: joinDFName // 要 Join 的 DataFrame 名字
 * joinExpr: col_a = col_b // Join 表达式
 * joinType: left // Join 方式
 * }}}
 */
@Operator(args = Array("joinExpr", "joinType"), source = Array("inDFName", "joinDFName"), msg = "join", name = "join")
case class JoinOperator(name: String,
                        defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                        pluginLst: Seq[PhPluginTrait[Column]])(implicit ctx: PhMapArgs[PhWorkArgs[_]])
        extends PhOperatorTrait[DataFrame] {
    /** 要作用的 DataFrame 名字 */
    val inDFName: String = defaultArgs.getAs[PhStringArgs]("inDFName").get.get
    /** 要 Join 的 DataFrame 名字 */
    val joinDFName: String = defaultArgs.getAs[PhStringArgs]("joinDFName").get.get
    /** Join 表达式 */
    val joinExpr: String = defaultArgs.getAs[PhStringArgs]("joinExpr").get.get
    /** Join 方式 */
    val joinType: String = defaultArgs.getAs[PhStringArgs]("joinType").get.get

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[DataFrame] = {
        val inDF = pr.getAs[PhDFArgs](inDFName).get.get
        val joinDF = pr.getAs[PhDFArgs](joinDFName).get.get
        val outDF = inDF.join(joinDF, expr(joinExpr), joinType)

        PhDFArgs(outDF)
    }
}
