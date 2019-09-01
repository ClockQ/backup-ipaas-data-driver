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

import com.pharbers.ipaas.data.driver.libs.spark.session.SparkConnInstance
import org.apache.spark.sql.DataFrame

/** SPARK 常用工具集，读取 avro 数据到 DataFrame
  *
  * @author cui
  * @version 0.1
  * @since 2019/7/04 10:28
  */
case class readAvro()(implicit val conn_instance: SparkConnInstance) extends SparkUtilTrait {
	/** 读取 avro 数据到 DataFrame
	  *
	  * @param file_path avro 文件路径
	  * @return _root_.org.apache.spark.sql.DataFrame 读取成功的数据集
	  * @author cui
	  * @version 0.1
	  * @since 2019/7/04 10:28
	  * @example 默认参数例子
	  * {{{
	  *  readAvro("hdfs:///test/read.avro")
	  * }}}
	  */
	def readAvro(file_path: String): DataFrame ={
		conn_instance.ss.read.format("com.databricks.spark.avro")
    		.load(file_path)
	}
}
