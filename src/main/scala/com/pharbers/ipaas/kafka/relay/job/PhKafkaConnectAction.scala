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

package com.pharbers.ipaas.kafka.relay.job

import java.util.UUID

import com.pharbers.ipaas.data.driver.api.factory.{PhFactoryTrait, getMethodMirror}
import com.pharbers.ipaas.data.driver.api.model.Action
import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.exceptions.{PhBuildJobException, PhOperatorException}
import com.pharbers.ipaas.data.driver.libs.log.PhLogDriver
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver

/** Kafka Connect Action 运行实体
  *
  * @param name        Kafka Connect Action 名字
  * @param defaultArgs 配置参数
  *                    {{{
  *                      protocol: http // 调用的 Kafka Connect HTTP 协议，默认是http
  *                      ip: 192.168.1.1 // 调用的 Kafka Connect HTTP ip
  *                      port: 8080 // 调用的 Kafka Connect HTTP 端口
  *                    }}}
  * @param operatorLst Kafka Connect Action 包含的 Operator 列表
  * @author clock
  * @version 0.1
  * @since 2019/7/5 12:20
  */
case class PhKafkaConnectAction(name: String,
                                defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                                operatorLst: Seq[PhOperatorTrait[Any]])
	extends PhActionTrait {
	/** Kafka Connect Action 执行入口
	  *
	  * @author clock
	  * @version 0.1
	  * @since 2019/7/5 12:20
	  */
	def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Any] = {
		val logFormat = pr.get("logFormat").asInstanceOf[PhFuncArgs[PhListArgs[PhStringArgs], PhStringArgs]].get
//		val chanelId = defaultArgs.get("chanelId").get.asInstanceOf[String] + UUID.randomUUID().toString.replaceAll("-", "")
		if (operatorLst.isEmpty) pr
		else operatorLst.foreach { oper => {
			logger.info(logFormat(oper.name))
//			oper.perform(PhMapArgs(pr.get ++ defaultArgs.get ++ Map("chanelId" -> PhStringArgs(chanelId))))
			oper.perform(PhMapArgs(pr.get ++ defaultArgs.get))
		}
		}

		PhNoneArgs
	}
}
