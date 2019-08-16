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

package com.pharbers.ipaas.kafka.relay.operators

import com.pharbers.ipaas.data.driver.api.work.{PhMapArgs, PhNoneArgs, PhOperatorTrait, PhPluginTrait, PhStringArgs, PhWorkArgs}
import com.pharbers.ipaas.kafka.relay.http.PhChanelHttpRequest
import org.apache.spark.sql.Column

import scala.util.parsing.json.JSON

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/08/06 16:12
  * @example 默认参数例子
  *          {{{
  *                "tasks.max" : "1", // 连接管道的最大线程数, 默认值为“1”
  *                "connection": "mongodb://192.168.100.176:27017", mongodb 地址
  *                "topic": "mongotest3", // 连接kafka的主题名字
  *                "database":"pharbers-aggrate-data", 数据库名
  *                "collection": "aggregateData",  集合名
  *                job: 之前aggmongo operator 的name
  *          }}}
  */
case class TMSourceFromMongodbOperator(name: String,
                                     defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                                     pluginLst: Seq[PhPluginTrait[Column]])
        extends PhOperatorTrait[Unit] {
    /** 调用的 HTTP 接口 */
    val api: String = "/connectors/"
    /** 使用的连接类 */
    val connectClass: String = defaultArgs.getAs[PhStringArgs]("connectClass") match {
        case Some(one) => one.get
        case None => "com.pharbers.kafka.connect.mongodb.MongodbSourceConnector"
    }
    /** 连接管道的最大线程数 */
    val tasksMax: String = defaultArgs.getAs[PhStringArgs]("tasksMax") match {
        case Some(one) => one.get
        case None => "1"
    }
    val connection: String = defaultArgs.getAs[PhStringArgs]("connection").get.get
    val database: String = defaultArgs.getAs[PhStringArgs]("database").get.get
    val collection: String = defaultArgs.getAs[PhStringArgs]("collection").get.get
    val jobName: String = defaultArgs.getAs[PhStringArgs]("job").get.get

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Unit] = {
        val job = pr.getAs[PhStringArgs](jobName).get.get
        /** 调用的 Kafka Connect HTTP 协议 */
        val protocol: String = pr.getAs[PhStringArgs]("protocol") match {
            case Some(one) => one.get
            case None => "http"
        }
        /** 调用的 Kafka Connect HTTP ip */
        val ip: String = pr.getAs[PhStringArgs]("ip").get.get
        /** 调用的 Kafka Connect HTTP 端口 */
        val port: String = pr.getAs[PhStringArgs]("port").get.get
        /** 管道的ID */
        val chanelId = pr.getAs[PhStringArgs]("chanelId").get.get
        val local = s"$protocol://$ip:$port"
        val postData =
            s"""
               			   |{
               			   |    "name": "$chanelId-source-connector",
               			   |    "config": {
               			   |        "connector.class": "$connectClass",
               			   |        "tasks.max": "$tasksMax",
               			   |        "topic": "source_$chanelId",
               			   |        "job": "$chanelId",
               			   |        "connection": "$connection",
               			   |        "database": "$database",
               			   |        "collection": "$collection",
               			   |        "filter": "{'job_id':'$job'}"
               			   |    }
               			   |}
	                 """.stripMargin

        val response = PhChanelHttpRequest(local + api, postData).getResponseAsStr
        if (response.code > 400) {
            val body = JSON.parseFull(response.body).get.asInstanceOf[Map[String, Any]]
            val errMsg = body("message").toString
            throw new Exception(errMsg)
        }
        PhNoneArgs
    }
}
