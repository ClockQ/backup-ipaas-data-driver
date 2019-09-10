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

/** 将DataFrame以avro的形式保存到HDFS的一个路径
  *
  * @author cui
  * @version 0.1
  * @since 2019/7/04 17:06
  */
case class save2Avro()(implicit val conn_instance: SparkConnInstance) extends SparkUtilTrait {
	/** 将DataFrame以avro的形式保存到HDFS的一个路径
	  *
	  * @param df 要保存的DataFrame
	  * @param path HDFS上的路径
	  * @return Unit
	  * @author cui
	  * @version 0.1
	  * @since 2019/7/04 10:28
	  * @example 默认参数例子
	  * {{{
	  *     df: DataFrame // 要保存的 DataFrame
	  *     path: String // HDFS上的路径
	  * }}}
	  */
	def save2Avro(df: DataFrame, path: String): Unit ={
		df.write.format("com.databricks.spark.avro").save(path)
	}
}
