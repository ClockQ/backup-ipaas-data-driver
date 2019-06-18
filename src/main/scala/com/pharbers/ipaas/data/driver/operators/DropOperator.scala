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

/** 删除数据集中的一列或多列
  *
  * @author clock
  * @version 0.1
  * @since 2019-06-18 10:21
  * @example 默认参数例子
  * {{{
  * inDFName: actionName // 要作用的 DataFrame 名字
  * drops: col_1#col_2 // 要删除的列名列表，用`#`号分割
  * }}}
  */
case class DropOperator(name: String,
                        defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                        pluginLst: Seq[PhPluginTrait2[Column]])
        extends PhOperatorTrait2[DataFrame] {
    /** 要作用的 DataFrame 名字 */
    val inDFName: String = defaultArgs.getAs[PhStringArgs]("inDFName").get.get
    /** 要删除的列名列表 */
    val drops: Array[String] = defaultArgs.getAs[PhStringArgs]("drops").get.get.split("#")

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[DataFrame] = {
        val inDF = pr.getAs[PhDFArgs](inDFName).get.get
        val outDF = inDF.drop(drops: _*)
//        phLog.setInfoLog(PhLogMsg("user", "traceID", "jobId", this.getClass.toString.split("\\.").last, "dec").toString)
        PhDFArgs(outDF)
    }
}