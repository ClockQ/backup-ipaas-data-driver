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
  * @since 2019/7/04 17:06
  */
case class save2Avro(implicit val conn_instance: SparkConnInstance) extends SparkUtilTrait {
	/** 将DataFrame以csv的形式保存到HDFS的一个路径
	  *
	  * @param df 要保存的DataFrame
	  * @param path HDFS上的路径
	  * @return Unit
	  * @author cui
	  * @version 0.1
	  * @since 2019-06-27 18:47
	  * @example 默认参数例子
	  * {{{
	  *     df: DataFrame // 要保存的 DataFrame
	  *     path: String // HDFS上的路径
	  *     delimiter: String // 分隔符，默认为 "#"
	  * }}}
	  */
	def save2Avro(df: DataFrame, path: String): Unit ={
		df.write.format("com.databricks.spark.avro").save(path)
	}
}
