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

/** SPARK 常用工具集，读取 MongoDB 数据到 DataFrame
  *
  * @author clock
  * @version 0.1
  * @since 2019/5/20 15:27
  * @note
  */
case class readMongo()(implicit val conn_instance: SparkConnInstance) extends SparkUtilTrait {
    /** 读取 MongoDB 数据到 DataFrame
      *
      * @param mongodbHost        mongodb 连接地址
      * @param mongodbPort        mongodb 连接端口
      * @param databaseName       读取的数据库名
      * @param collName           读取的集合名
      * @param readPreferenceName 读取模式
      * @return _root_.org.apache.spark.sql.DataFrame 读取成功的数据集
      * @author clock
      * @version 0.1
      * @since 2019/6/17 11:28
      * @example 默认参数例子
      *          {{{
      *           readMongo("mongodb_host", "27017", "test_db", "test_coll", "secondaryPreferred")
      *          }}}
      */
    def readMongo(mongodbHost: String,
                  mongodbPort: String,
                  databaseName: String,
                  collName: String,
                  readPreferenceName: String = "secondaryPreferred"): DataFrame = {
        conn_instance.ss.read.format("com.mongodb.spark.sql.DefaultSource")
                .option("spark.mongodb.input.uri", s"mongodb://$mongodbHost:$mongodbPort/")
                .option("spark.mongodb.input.database", databaseName)
                .option("spark.mongodb.input.collection", collName)
                .option("readPreference.name", readPreferenceName)
                .load()
    }
}