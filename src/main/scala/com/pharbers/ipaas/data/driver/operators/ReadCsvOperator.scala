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

import org.apache.spark.sql.DataFrame
import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.libs.spark.util.readCsv
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver

/** 读取 CSV 的算子
 *
 * @author clock
 * @version 0.1
 * @since 2019/6/15 17:59
 * @example 默认参数例子
 * {{{
 *       path: hdfs:///test.csv //CSV 的路径
 *       delimiter: "," //CSV 的分隔符，默认为 31.toChar.toString
 * }}}
 */
case class ReadCsvOperator(name: String,
                           defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                           pluginLst: Seq[PhPluginTrait[Any]])(implicit ctx: PhMapArgs[PhWorkArgs[_]])
        extends PhOperatorTrait[DataFrame] {

    /** spark driver 实例 */
    val sd: PhSparkDriver = ctx.get("sparkDriver").asInstanceOf[PhSparkDriverArgs].get

    /** CSV 的路径 */
    val path: String = defaultArgs.getAs[PhStringArgs]("path").get.get
    /** CSV 的分隔符，默认为 31.toChar.toString */
    val delimiter: String = defaultArgs.getAs[PhStringArgs]("delimiter") match {
        case Some(one) => one.get
        case _ => 31.toChar.toString
    }

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[DataFrame] = {
        PhDFArgs(sd.setUtil(readCsv()(sd)).readCsv(path, delimiter))
    }
}
