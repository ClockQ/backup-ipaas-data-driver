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

import java.util.concurrent.TimeUnit

import com.pharbers.ipaas.data.driver.api.work.{PhLogDriverArgs, PhMapArgs, PhNoneArgs, PhOperatorTrait, PhPluginTrait, PhStringArgs, PhWorkArgs}
import com.pharbers.ipaas.data.driver.libs.log.PhLogDriver
import com.pharbers.kafka.consumer.PharbersKafkaConsumer
import com.pharbers.kafka.producer.PharbersKafkaProducer
import com.pharbers.kafka.schema.{MonitorRequest, MonitorResponse}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.Column
import scalaj.http.Http

case class MonitorOperator(name: String,
                           defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                           pluginLst: Seq[PhPluginTrait[Column]])
	extends PhOperatorTrait[String] {
//	extends PhOperatorTrait[Unit] {

	var listenMonitor: Boolean = false

//	override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Unit] = {
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
		val api: String = "/connectors/"
		//step 3 向MonitorServer发送这次JobID的监控请求（Kafka Producer）（前提要确保MonitorServer已经启动!）
		// 请求参数（[JobID]和[监控策略]）
		def sendMonitorRequest(): Unit = {
			val pkp = new PharbersKafkaProducer[String, MonitorRequest]
			val record = new MonitorRequest(chanelId, "default")
			val fu = pkp.produce("MonitorRequest", chanelId, record)
			log.setInfoLog(fu.get(10, TimeUnit.SECONDS))
			pkp.producer.close()
		}

		def myProcess(record: ConsumerRecord[String, MonitorResponse]): Unit = {
			log.setInfoLog("===myProcess>>>" + record.key() + ":" + record.value().toString)
			if (record.value().getProgress == 100 && record.value().getJobId.toString == chanelId) {
				listenMonitor = false
			}
			if (record.value().getError.toString != "" && record.value().getJobId.toString == chanelId) {
				log.setInfoLog(s"收到错误信息后关闭，id: ${record.value().getJobId.toString}, error：${record.value().getError.toString}")
				listenMonitor = false
			}
		}

		//step 4 向MonitorServer拉取进度和处理情况（Kafka Consumer）（前提要确保MonitorServer已经启动!）
		def pollMonitorProgress(chanelId: String): Unit = {
			var time = 0
			listenMonitor = true
			val pkc = new PharbersKafkaConsumer[String, MonitorResponse](List("MonitorResponse"), 1000, Int.MaxValue, myProcess)
			val t = new Thread(pkc)

			try {
				log.setInfoLog("PollMonitorProgress starting!")
				t.start()
				log.setInfoLog("PollMonitorProgress is started! Close by enter \"exit\" in console.")
				//            var cmd = Console.readLine()
				while (listenMonitor) {
//					Thread.sleep(30000)
					Thread.sleep(10000)
					time = time + 1
//					if (time > 50) {
					if (time > 2) {
						log.setInfoLog("error: 程序异常")
						listenMonitor = false
					}
				}
			} catch {
				case ie: InterruptedException => {
					log.setInfoLog(ie.getMessage)
				}
			} finally {
				pkc.close()
				log.setInfoLog("PollMonitorProgress close!")
			}
		}
		sendMonitorRequest()
		pollMonitorProgress(chanelId)
		PhStringArgs(chanelId)
	}
}
