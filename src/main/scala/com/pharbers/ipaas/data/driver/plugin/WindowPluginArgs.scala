package com.pharbers.ipaas.data.driver.plugin

import com.pharbers.ipaas.data.driver.api.work.{PhMapArgs, PhOperatorTrait, PhStringArgs, PhWorkArgs}

/** 使用窗口函数插件的共用参数基类
  * @author dcs
  * @param args PhMapArgs
  * @tparam T (map(valueColumnName -> 要计算的列名，dateColName -> 日期的列名，partitionColumnNames -> 用来分组的一列或多列的列名))
  */
abstract class WindowPluginArgs[T <: Map[String, PhWorkArgs[_]]](args: PhWorkArgs[T]) {
    val valueColumnName: String = args.get.getOrElse("valueColumnName",throw new Exception("not found valueColumnName")).get.asInstanceOf[String]
    val dateColName: String = args.get.getOrElse("dateColName",throw new Exception("not found dateColName")).get.asInstanceOf[String]
    val partitionColumnNames: List[String] = args.get.getOrElse("partitionColumnNames",throw new Exception("not found partitionColumnNames")).get.asInstanceOf[List[PhStringArgs]].map(x => x.get)

}
