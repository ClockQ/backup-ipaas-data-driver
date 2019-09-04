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

/** SPARK 常用工具集，读取 ES 数据到 DataFrame
 *
 * @author clock
 * @version 0.1
 * @since 2019/9/4 16:31
 * @note
 */
case class readEs()(implicit val conn_instance: SparkConnInstance) extends SparkUtilTrait {
    /** 读取 ES 数据到 DataFrame
     *
     * @param esHost es 连接地址
     * @param esPort es 连接端口
     * @param index  读取的索引名
     * @param query  查询语句，默认为空
     * @return _root_.org.apache.spark.sql.DataFrame 读取成功的数据集
     * @author clock
     * @version 0.1
     * @since 2019/9/4 16:31
     * @note query语法
     *       uri查询： https://www.elastic.co/guide/en/elasticsearch/reference/7.2/search-uri-request.html
     *       dsl查询： https://www.elastic.co/guide/en/elasticsearch/reference/7.2/search-request-body.html
     * @example 默认参数例子
     * {{{
     *  readMongo("es_host", "9200", "test_index", "?q=name:IronMan")
     *  readMongo("es_host", "9200", "test_index", """{ "query": {"match_all": {} } }""")
     * }}}
     */
    def readEs(esHost: String,
               esPort: String,
               index: String,
               query: String): DataFrame = {
        conn_instance.ss.read.format("org.elasticsearch.spark.sql")
                .option("es.nodes.wan.only", "true")
                .option("es.pushdown", "true")
                .option("es.nodes", esHost)
                .option("es.port", esPort)
                .option("es.resource", index)
                .option("es.query", query)
                .load()
    }
}
