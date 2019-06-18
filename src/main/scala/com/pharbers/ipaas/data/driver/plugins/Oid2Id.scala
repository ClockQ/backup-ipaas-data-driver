//package com.pharbers.ipaas.data.driver.plugins
//
//import com.pharbers.ipaas.data.driver.api.work.{PhColArgs, PhMapArgs, PhNoneArgs, PhPluginTrait, PhStringArgs, PhWorkArgs}
//import org.apache.spark.sql.functions._
//
///** 功能描述
//  *
//  * @author EDZ
//  * @param
//  * @tparam
//  * @note
//  */
//case class Oid2Id(override val name: String = "Id2Oid",
//             override val defaultArgs: PhWorkArgs[_] = PhNoneArgs) extends PhPluginTrait {
//
//
//
//    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
//        val argsMap = defaultArgs.asInstanceOf[PhMapArgs[_]]
//        val oidColName = argsMap.getAs[PhStringArgs]("oidColName").get.get
//        PhColArgs(lit(col(oidColName)("oid")))
//    }
//}