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

import com.pharbers.ipaas.data.driver.api.work.{PhMapArgs, PhOperatorTrait, PhPluginTrait, PhStringArgs, PhWorkArgs}
import org.apache.spark.sql.Column

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/08/16 10:35
  * @note 一些值得注意的地方
  */
case class TMMongodbReportAggOperator(name: String,
                                 defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                                 pluginLst: Seq[PhPluginTrait[Column]])
        extends PhOperatorTrait[String] {

    val connection: String = defaultArgs.getAs[PhStringArgs]("connection").get.get
    val database: String = defaultArgs.getAs[PhStringArgs]("database").get.get
    val collection: String = defaultArgs.getAs[PhStringArgs]("collection").get.get
    val periodId: String = defaultArgs.getAs[PhStringArgs]("periodId").get.get
    val projectId: String = defaultArgs.getAs[PhStringArgs]("projectId").get.get
    val proposalId: String = defaultArgs.getAs[PhStringArgs]("proposalId").get.get

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[String] = {
        import com.pharbers.NTMIOAggregation._
        val res = TmReportAgg(proposalId, projectId, periodId)
        PhStringArgs(res)
    }
}
