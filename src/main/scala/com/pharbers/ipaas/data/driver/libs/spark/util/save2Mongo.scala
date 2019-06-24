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

import org.apache.spark.sql.{DataFrame, SaveMode}
import com.pharbers.ipaas.data.driver.libs.spark.session.SparkConnInstance

/** SPARK 常用工具集，保存 DataFrame 数据到 MongoDB
  *
  * @author clock
  * @version 0.1
  * @since 2019/5/20 15:27
  * @note
  */
case class save2Mongo(implicit val conn_instance: SparkConnInstance) extends SparkUtilTrait {

    /** 保存 DataFrame 数据到 MongoDB
      *
      * @param dataFrame 要保存的数据集
      * @param mongodbHost mongodb 连接地址
      * @param mongodbPort mongodb 连接端口
      * @param databaseName 保存的数据库名
      * @param collName 保存的集合名
      * @param saveMode 保存模式
      * @return Unit
      * @author clock
      * @version 0.1
      * @since 2019/6/17 11:23
      * @example 默认参数例子
      * {{{
      * save2Mongo(df, "mongodb_host", "27017", "test_db", "test_coll", SaveMode.Append)
      * }}}
      */
    def save2Mongo(dataFrame: DataFrame,
                   mongodbHost: String,
                   mongodbPort: String,
                   databaseName: String,
                   collName: String,
                   saveMode: SaveMode = SaveMode.Append): Unit = {
        dataFrame.write.format("com.mongodb.spark.sql.DefaultSource").mode(saveMode)
                .option("uri", s"mongodb://$mongodbHost:$mongodbPort/")
                .option("database", databaseName)
                .option("collection", collName)
                .save()
    }
}