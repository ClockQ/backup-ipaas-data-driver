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
import org.apache.spark.sql.{DataFrame, SaveMode}

/** SPARK 常用工具集，保存 DataFrame 数据到 ES
 *
 * @author clock
 * @version 0.1
 * @since 2019/9/4 16:41
 * @note
 */
case class save2Es()(implicit val conn_instance: SparkConnInstance) extends SparkUtilTrait {

    /** 保存 DataFrame 数据到 ES
     *
     * @param dataFrame 要保存的数据集
     * @param esHost    es 连接地址
     * @param esPort    es 连接端口
     * @param index     读取的索引名
     * @param saveMode  保存模式
     * @return Unit
     * @author clock
     * @version 0.1
     * @since 2019/9/4 16:41
     * @example 默认参数例子
     * {{{
     *  save2Es(df, "es_host", "9200", "test_index", SaveMode.Append)
     * }}}
     */
    def save2Es(dataFrame: DataFrame,
                esHost: String,
                esPort: String,
                index: String,
                saveMode: SaveMode = SaveMode.Append): Unit = {
        dataFrame.write.format("org.elasticsearch.spark.sql")
                .option("es.nodes.wan.only", "true")
                .option("es.pushdown", "true")
                .option("es.index.auto.create", "true")
                .option("es.nodes", esHost)
                .option("es.port", esPort)
                .option("es.resource", index)
                .mode(saveMode)
                .save()
    }
}
