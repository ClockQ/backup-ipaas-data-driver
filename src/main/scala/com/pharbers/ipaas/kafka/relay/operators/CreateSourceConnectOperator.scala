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

import scalaj.http.Http
import scala.util.parsing.json._
import org.apache.spark.sql.Column
import com.pharbers.ipaas.data.driver.api.work._

/** create source connect 算子
  *
  * @author clock
  * @version 0.1
  * @since 2019/7/3 18:10
  * @example 默认参数例子
  * {{{
  * connectName: "testSourceConnect" // 创建的source连接名
  * connectClass: "org.apache.kafka.connect.file.FileStreamSourceConnector" // 指定使用的连接类，有默认值
  * tasksMax: "1" // 连接管道的最大线程数, 默认值为“1”
  * topic: "testTopic" // 连接kafka的主题名字
  * listenFile: "/data/test.log" // 监控的文件路径
  * }}}
  */
case class CreateSourceConnectOperator(name: String,
                                       defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                                       pluginLst: Seq[PhPluginTrait[Column]])
        extends PhOperatorTrait[String => Unit] {
    /** 调用的 HTTP 接口 */
    val api: String = "/connectors/"
    /** 创建的source连接名 */
    val connectName: String = defaultArgs.getAs[PhStringArgs]("connectName").get.get
    /** 使用的连接类 */
    val connectClass: String = defaultArgs.getAs[PhStringArgs]("connectClass") match {
        case Some(one) => one.get
        case None => "org.apache.kafka.connect.file.FileStreamSourceConnector"
    }
    /** 连接管道的最大线程数 */
    val tasksMax: String = defaultArgs.getAs[PhStringArgs]("tasksMax") match {
        case Some(one) => one.get
        case None => "1"
    }
    /** 连接kafka的主题名字 */
    val topic: String = defaultArgs.getAs[PhStringArgs]("topic").get.get
    /** 监控的文件路径 */
    val listenFile: String = defaultArgs.getAs[PhStringArgs]("listenFile").get.get

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[String => Unit] = {

        val postData =
            s"""
               |{
               |    "name": "$connectName",
               |    "config": {
               |        "connector.class": "$connectClass",
               |        "tasks.max": "$tasksMax",
               |        "topic": "$topic",
               |        "file": "$listenFile"
               |    }
               |}
             """.stripMargin

        val call: String => Unit = local => {
            val response = Http(local + api).postData(postData).header("Content-Type", "application/json").asString
            if(response.code > 400){
                val body = JSON.parseFull(response.body).get.asInstanceOf[Map[String, Any]]
                val errMsg = body("message").toString
                throw new Exception(errMsg)
            }
        }

        PhFuncArgs(call)
    }
}
