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
import com.pharbers.ipaas.data.driver.libs.log.PhLogDriver
import org.apache.spark.sql.Column
import scalaj.http.Http

import scala.util.parsing.json._

/** delect connect 算子
  *
  * @author clock
  * @version 0.1
  * @since 2019/7/3 19:10
  * @example 默认参数例子
  * {{{
  *  connectName: "testName" // 删除的连接名
  * }}}
  */
case class DeleteConnectOperator(name: String,
                                 defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                                 pluginLst: Seq[PhPluginTrait[Column]])
	extends PhOperatorTrait[String] {

	val api: String = "/connectors/"

	override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[String] = {
		val log: PhLogDriver = pr.get("logDriver").asInstanceOf[PhLogDriverArgs].get
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
		try {
			val deleteSourceConnectorResult = Http(local + api + s"$chanelId-source-connector").method("DELETE").asString
			if (deleteSourceConnectorResult.code > 400) {
				log.setErrorLog(deleteSourceConnectorResult)
				val body = JSON.parseFull(deleteSourceConnectorResult.body).get.asInstanceOf[Map[String, Any]]
				val errMsg = body("message").toString
				throw new Exception(errMsg)
			} else log.setInfoLog(deleteSourceConnectorResult)
			val deleteSinkConnectorResult = Http(local + api + s"$chanelId-sink-connector").method("DELETE").asString
			if (deleteSinkConnectorResult.code > 400) {
				log.setErrorLog(deleteSinkConnectorResult)
				val body = JSON.parseFull(deleteSinkConnectorResult.body).get.asInstanceOf[Map[String, Any]]
				val errMsg = body("message").toString
				throw new Exception(errMsg)
			} else log.setInfoLog(deleteSinkConnectorResult)
		} catch {
			case e: Exception => log.setInfoLog(e.getMessage)
		}

		PhStringArgs(chanelId)
	}
}
