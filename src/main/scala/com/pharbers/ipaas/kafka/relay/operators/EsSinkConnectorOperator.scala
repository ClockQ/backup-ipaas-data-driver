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
  * @since 2019/08/16 12:36
  * @note 一些值得注意的地方
  */
case class EsSinkConnectorOperator(name: String,
                                   defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                                   pluginLst: Seq[PhPluginTrait[Column]])
        extends PhOperatorTrait[Unit] {

    /** 调用的 HTTP 接口 */
    val api: String = "/connectors/"
    /** 连接管道的最大线程数 */
    val tasksMax: String = defaultArgs.getAs[PhStringArgs]("tasksMax") match {
        case Some(one) => one.get
        case None => "1"
    }
    /** sink的文件路径 */
    val esUrl: String = defaultArgs.getAs[PhStringArgs]("esUrl").get.get
    /** source topic */
    val topic: String = defaultArgs.getAs[PhStringArgs]("topic").get.get

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
        val local = s"$protocol://$ip:$port"
        val chanelId = pr.getAs[PhStringArgs]("chanelId").get.get
        println("chanelId:  " + chanelId + "开始=========")
        val postData =
            s"""
               			   |{
               			   |    "name": "$chanelId-sink-connector",
               			   |    "config": {
               			   |        "topics": "$topic",
               			   |        "jobId": "$chanelId",
               			   |        "connector.class": "com.pharbers.kafka.connect.elasticsearch.ElasticsearchSinkConnector",
               			   |		"tasks.max": $tasksMax,
               			   |		"key.ignore": "true",
               			   |		"connection.url": "$esUrl",
               			   |		"type.name": "",
               			   |        "read.timeout.ms": "10000",
               			   |        "connection.timeout.ms": "5000"
               			   |    }
               			   |}
             """.stripMargin

        val response = PhChanelHttpRequest(local + api, postData).getResponseAsStr
        if (response.code > 400) {
            val body = JSON.parseFull(response.body).get.asInstanceOf[Map[String, Any]]
            val errMsg = body("message").toString
            throw new Exception(errMsg)
        }
        //todo: 没有监控，暂时这样
        Thread.sleep(10000)
        PhNoneArgs
    }
}
