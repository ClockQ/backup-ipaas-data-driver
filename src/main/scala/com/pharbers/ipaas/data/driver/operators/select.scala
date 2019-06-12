package com.pharbers.ipaas.data.driver.operators

import com.pharbers.ipaas.data.driver.api.work._
import org.apache.spark.sql.functions._

/** 功能描述
  * select算子
  * @param plugin 插件
  * @param name 算子 name
  * @param defaultArgs 配置参数 "inDFName"-> pr中的df名, "selects" -> select的列用 # 分割
  * @author dcs
  * @version 0.0
  * @since 2019/6/11 16:50
  * @note 一些值得注意的地方
  */
case class select(plugin: PhPluginTrait, name: String, defaultArgs: PhWorkArgs[_]) extends PhOperatorTrait {
    val defaultMapArgs: PhMapArgs[PhWorkArgs[_]] = defaultArgs.toMapArgs[PhWorkArgs[_]]
    val inDFName: String = defaultMapArgs.getAs[PhStringArgs]("inDFName").get.get
    val selectLst: Array[String] = defaultMapArgs.getAs[PhStringArgs]("selects").get.get.split("#")

    /** 功能描述
      *select

      * @param pr 运行时储存之前action和算子所在action之前算子的结果
      * @return _root_.com.pharbers.ipaas.data.driver.api.work.PhWorkArgs[_]
      * @author EDZ
      * @version 0.0
      * @since 2019/6/11 17:14
      * @note 一些值得注意的地方
      * @example {{{这是一个例子}}}
      */
    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        val prMapArgs = pr.toMapArgs[PhWorkArgs[_]]
        val inDF = prMapArgs.getAs[PhDFArgs](inDFName).get.get
        val outDF = inDF.selectExpr(selectLst: _*)

        PhDFArgs(outDF)
    }
}
