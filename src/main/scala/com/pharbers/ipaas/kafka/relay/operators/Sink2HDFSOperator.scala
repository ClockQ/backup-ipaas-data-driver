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

import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.kafka.relay.http.PhChanelHttpRequest
import org.apache.spark.sql.Column

import scala.util.parsing.json._

/** create sink connect 算子
  *
  * @author clock
  * @version 0.1
  * @since 2019/7/3 19:10
  * @example 默认参数例子
  * {{{
  *     connectName: "testSinkConnect" // 创建的sink连接名
  *     connectClass: "org.apache.kafka.connect.file.FileStreamSinkConnector" // 指定使用的连接类，有默认值
  *     tasksMax: "1" // 连接管道的最大线程数, 默认值为“1”
  *     topic: "testTopic" // 连接kafka的主题名字
  *     listenFile: "/data/test.log" // sink的文件路径
  * }}}
  */
case class Sink2HDFSOperator(name: String,
                             defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                             pluginLst: Seq[PhPluginTrait[Column]])
	extends PhOperatorTrait[Unit] {
	/** 调用的 HTTP 接口 */
	val api: String = "/connectors/"
	/** 使用的连接类 */
	val connectClass: String = defaultArgs.getAs[PhStringArgs]("connectClass") match {
		case Some(one) => one.get
		case None => "io.confluent.connect.hdfs.HdfsSinkConnector"
	}
	/** 连接管道的最大线程数 */
	val tasksMax: String = defaultArgs.getAs[PhStringArgs]("tasksMax") match {
		case Some(one) => one.get
		case None => "1"
	}
	/** sink的文件路径 */
	val hdfsPath: String = defaultArgs.getAs[PhStringArgs]("hdfsPath").get.get

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
			   |    "name": "$chanelId-sink-connector",
			   |    "config": {
			   |        "connector.class": "$connectClass",
			   |        "tasks.max": "$tasksMax",
			   |        "topics": "source_$chanelId",
			   |        "jobId": "$chanelId",
			   |        "hdfs.url": "$hdfsPath",
			   |        "format.class": "io.confluent.connect.hdfs.parquet.ParquetFormat",
			   |        "rotate.interval.ms":1000,
			   |    	"flush.size": "2000"
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
