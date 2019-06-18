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
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.libs.spark.util.readParquet

/** 读取 Parquet 的算子
  *
  * @author clock
  * @version 0.1
  * @since 2019/6/15 18:59
  * @example 默认参数例子
  * {{{
  *        path: hdfs:///test //Parquet 的路径
  * }}}
  */
case class ReadParquetOperator(name: String,
                               defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                               pluginLst: Seq[PhPluginTrait2[Any]])
        extends PhOperatorTrait2[DataFrame] {

    /** Parquet 的路径 */
    val path: String = defaultArgs.getAs[PhStringArgs]("path").get.get

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[DataFrame] = {
        implicit val sd: PhSparkDriver = pr.get("sparkDriver").asInstanceOf[PhSparkDriverArgs].get
        PhDFArgs(sd.setUtil(readParquet()).readParquet(path))
    }
}
