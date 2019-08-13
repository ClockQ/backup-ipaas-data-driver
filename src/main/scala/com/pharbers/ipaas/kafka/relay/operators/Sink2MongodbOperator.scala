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
  *                "key.converter":"io.confluent.connect.avro.AvroConverter",
  *               "key.converter.schema.registry.url":"http://59.110.31.50:8081",
  *               "value.converter":"io.confluent.connect.avro.AvroConverter",
  *               "value.converter.schema.registry.url":"http://59.110.31.50:8081",
  *              "connector.class": "at.grahsl.kafka.connect.mongodb.MongoDbSinkConnector",
  *               "topics": "aggregate_data_003",
  *               "jobId": "aggregate_data_003",
  *               "mongodb.connection.uri": "mongodb://192.168.100.176:27017/kafkaconnect?w=1&journal=true",
  *               "mongodb.collection": "aggregate_data_003"
  *          }}}
  */
case class Sink2MongodbOperator(name: String,
                                     defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                                     pluginLst: Seq[PhPluginTrait[Column]])
        extends PhOperatorTrait[Unit] {
    /** 调用的 HTTP 接口 */
    val api: String = "/connectors/"
    /** 使用的连接类 */
    val connectClass: String = defaultArgs.getAs[PhStringArgs]("connectClass") match {
        case Some(one) => one.get
        case None => "at.grahsl.kafka.connect.mongodb.MongoDbSinkConnector"
    }
    /** 连接管道的最大线程数 */
    val tasksMax: String = defaultArgs.getAs[PhStringArgs]("tasksMax") match {
        case Some(one) => one.get
        case None => "1"
    }
    val connection: String = defaultArgs.getAs[PhStringArgs]("connection").get.get
    val collection: String = defaultArgs.getAs[PhStringArgs]("collection").get.get

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Unit] = {
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
                           |        "key.converter":"io.confluent.connect.avro.AvroConverter",
                           |        "key.converter.schema.registry.url":"http://59.110.31.50:8081",
                           |        "value.converter":"io.confluent.connect.avro.AvroConverter",
                           |        "value.converter.schema.registry.url":"http://59.110.31.50:8081",
               			   |        "connector.class": "$connectClass",
               			   |        "tasks.max": "$tasksMax",
               			   |        "topic": "source_$chanelId",
               			   |        "job": "$chanelId",
               			   |        "mongodb.connection.uri": "$connection",
               			   |        "mongodb.collection": "$collection"
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
