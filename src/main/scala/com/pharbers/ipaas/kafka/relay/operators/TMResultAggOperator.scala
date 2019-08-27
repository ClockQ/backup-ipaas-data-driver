///*
// * This file is part of com.pharbers.ipaas-data-driver.
// *
// * com.pharbers.ipaas-data-driver is free software: you can redistribute it and/or modify
// * it under the terms of the GNU General Public License as published by
// * the Free Software Foundation, either version 3 of the License, or
// * (at your option) any later version.
// *
// * com.pharbers.ipaas-data-driver is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with Foobar.  If not, see <https://www.gnu.org/licenses/>.
// */
//
//package com.pharbers.ipaas.kafka.relay.operators
//
//import com.pharbers.TmAggregation.TmAggCal2Report
//import com.pharbers.ipaas.data.driver.api.work.{PhMapArgs, PhNoneArgs, PhOperatorTrait, PhPluginTrait, PhStringArgs, PhWorkArgs}
//import org.apache.spark.sql.Column
//
///** 功能描述
//  *
//  * @param args 构造参数
//  * @tparam T 构造泛型参数
//  * @author dcs
//  * @version 0.0
//  * @since 2019/08/14 10:30
//  * @note jobName 开始时mongodb agg action name
//  */
//case class TMResultAggOperator(name: String,
//                               defaultArgs: PhMapArgs[PhWorkArgs[Any]],
//                               pluginLst: Seq[PhPluginTrait[Column]])
//        extends PhOperatorTrait[Unit] {
//
//    val jobName: String = defaultArgs.getAs[PhStringArgs]("job").get.get
//    val periodId: String = defaultArgs.getAs[PhStringArgs]("periodId").get.get
//    val projectId: String = defaultArgs.getAs[PhStringArgs]("projectId").get.get
//    val proposalId: String = defaultArgs.getAs[PhStringArgs]("proposalId").get.get
//    val phase: Int = defaultArgs.getAs[PhStringArgs]("phase").getOrElse(PhStringArgs("0")).get.toInt
//
//
//    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Unit] = {
//        val job = pr.getAs[PhStringArgs](jobName).get.get
//        TmAggCal2Report.apply(job, proposalId, projectId, periodId, phase)
//        PhNoneArgs
//    }
//}
