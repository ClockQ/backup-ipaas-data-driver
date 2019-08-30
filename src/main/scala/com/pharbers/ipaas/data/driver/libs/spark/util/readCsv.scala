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

package com.pharbers.ipaas.data.driver.libs.spark.util

import org.apache.spark.sql.DataFrame
import com.pharbers.ipaas.data.driver.libs.spark.session.SparkConnInstance

/** SPARK 常用工具集，读取 CSV 数据到 DataFrame
  *
  * @author clock
  * @version 0.1
  * @since 2019/5/20 15:27
  * @note
  */
case class readCsv()(implicit val conn_instance: SparkConnInstance) extends SparkUtilTrait {
    /** 读取 CSV 数据到 DataFrame
      *
      * @param file_path CSV 文件路径
      * @param delimiter CSV 分割符
      * @param header    是否处理头部
      * @return _root_.org.apache.spark.sql.DataFrame 读取成功的数据集
      * @author clock
      * @version 0.1
      * @since 2019/6/17 11:30
      * @example 默认参数例子
      *          {{{
      *           readCsv("hdfs:///test/read.csv", ",", true)
      *          }}}
      */
    def readCsv(file_path: String, delimiter: String = ",", header: Boolean = true): DataFrame = {
        conn_instance.ss.read.format("com.databricks.spark.csv")
                .option("header", header)
                .option("delimiter", delimiter)
                .load(file_path)
    }
}