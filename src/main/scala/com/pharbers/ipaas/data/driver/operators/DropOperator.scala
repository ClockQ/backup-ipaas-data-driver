package com.pharbers.ipaas.data.driver.operators

import com.pharbers.ipaas.data.driver.api.work._
import org.apache.spark.sql.functions._
/** 功能描述
  * 删除列算子
  * @param plugin 插件
  * @param name 算子 name
  * @param defaultArgs 配置参数 "inDFName"-> pr中的df名， "dropColName" -> 删除的列名
  * @author dcs
  * @version 0.0
  * @since 2019/6/11 16:50
  * @note 一些值得注意的地方
  */
case class DropOperator(plugin: PhPluginTrait, name: String, defaultArgs: PhWorkArgs[_]) extends PhOperatorTrait {

    /** 功能描述
      *删除列

      * @param pr 运行时储存之前action和算子所在action之前算子的结果
      * @return _root_.com.pharbers.ipaas.data.driver.api.work.PhWorkArgs[_]
      * @author EDZ
      * @version 0.0
      * @since 2019/6/11 17:14
      * @note 一些值得注意的地方
      * @example {{{这是一个例子}}}
      */
    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        val defaultMapArgs = defaultArgs.toMapArgs[PhWorkArgs[_]]
        val prMapArgs = pr.toMapArgs[PhWorkArgs[_]]
        val inDFName = defaultMapArgs.getAs[PhStringArgs]("inDFName").get.get
        val dropColName = defaultMapArgs.getAs[PhStringArgs]("dropColName").get.get
        val inDF = prMapArgs.getAs[PhDFArgs](inDFName).get.get
        val outDF = inDF.drop(dropColName)

        PhDFArgs(outDF)
    }
}