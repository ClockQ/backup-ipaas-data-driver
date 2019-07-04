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
  * connectName: "testName" // 删除的连接名
  * }}}
  */
case class DeleteConnectOperator(name: String,
                                 defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                                 pluginLst: Seq[PhPluginTrait[Column]])
        extends PhOperatorTrait[String => Unit] {

    val api: String = "/connectors/"
    val connect_name: String = defaultArgs.getAs[PhStringArgs]("connectName").get.get

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[String => Unit] = {

        val call: String => Unit = local => {
            val response = Http(local+api+connect_name).method("DELETE").asString
            if(response.code > 400){
                val body = JSON.parseFull(response.body).get.asInstanceOf[Map[String, Any]]
                val errMsg = body("message").toString
                throw new Exception(errMsg)
            }
        }

        PhFuncArgs(call)
    }
}
