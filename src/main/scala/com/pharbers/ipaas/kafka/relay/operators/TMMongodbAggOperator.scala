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
//import com.pharbers.TmAggregation.TmAggPreset2Cal
//import com.pharbers.ipaas.data.driver.api.work.{PhMapArgs, PhOperatorTrait, PhPluginTrait, PhStringArgs, PhWorkArgs}
//import org.apache.spark.sql.Column
//
///** 功能描述
//  *
//  * @param args 构造参数
//  * @tparam T 构造泛型参数
//  * @author dcs
//  * @version 0.0
//  * @since 2019/08/13 15:53
//  * @note 一些值得注意的地方
//  */
//case class TMMongodbAggOperator(name: String,
//                           defaultArgs: PhMapArgs[PhWorkArgs[Any]],
//                           pluginLst: Seq[PhPluginTrait[Column]])
//        extends PhOperatorTrait[String] {
//
//    val connection: String = defaultArgs.getAs[PhStringArgs]("connection").get.get
//    val database: String = defaultArgs.getAs[PhStringArgs]("database").get.get
//    val collection: String = defaultArgs.getAs[PhStringArgs]("collection").get.get
//    val periodId: String = defaultArgs.getAs[PhStringArgs]("periodId").get.get
//    val projectId: String = defaultArgs.getAs[PhStringArgs]("projectId").get.get
//    val proposalId: String = defaultArgs.getAs[PhStringArgs]("proposalId").get.get
//    val phase: Int = defaultArgs.getAs[PhStringArgs]("phase").getOrElse(PhStringArgs("0")).get.toInt
//
//    /** CMD执行方法
//      *
//      * @param pr 包含的子的 CMD 执行结果及之前执行过的 Action 中的结果
//      * @return _root_.com.pharbers.ipaas.data.driver.api.work.PhWorkArgs[A]
//      * @author clock
//      * @version 0.1
//      * @since 2019/6/15 15:24
//      * @note
//      * {{{
//      *     pr中需要传递 `key` 为 `sparkDriver` 的 PhSparkDriverArgs
//      *     pr中需要传递 `key` 为 `logDriver` 的 PhLogDriverArgs
//      * }}}
//      */
//    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[String] = {
//        import com.pharbers.TmAggregation._
//        val res = TmAggPreset2Cal.apply(proposalId, projectId, periodId, phase)
//        PhStringArgs(res)
//    }
//}
