//package com.pharbers.ipaas.data.driver.operators
//
//import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhMapArgs, PhNoneArgs, PhOperatorTrait, PhPluginTrait, PhStringArgs, PhWorkArgs}
//import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
//import com.pharbers.ipaas.data.driver.libs.spark.util.{readCsv, save2Parquet}
//import env.sparkObj2
//
///** 功能描述
//  * 存Parquet算子
//  * @param plugin 插件
//  * @param name 算子 name
//  * @param defaultArgs 配置参数 "inDFName"-> pr中的df名, "path" -> 路径
//  * @author dcs
//  * @version 0.0
//  * @since 2019/6/11 16:50
//  * @note 一些值得注意的地方
//  */
//case class SaveParquetOperator(plugin: PhPluginTrait, name: String, defaultArgs: PhWorkArgs[_]) extends PhOperatorTrait{
//    val defaultMapArgs: PhMapArgs[PhWorkArgs[_]] = defaultArgs.toMapArgs[PhWorkArgs[_]]
//    val inDFName: String = defaultMapArgs.getAs[PhStringArgs]("inDFName").get.get
//    val path: String = defaultMapArgs.getAs[PhStringArgs]("path").get.get
//
//    /** 功能描述
//      *存Parquet
//
//      * @param pr 运行时储存之前action和算子所在action之前算子的结果
//      * @return _root_.com.pharbers.ipaas.data.driver.api.work.PhWorkArgs[_]
//      * @author EDZ
//      * @version 0.0
//      * @since 2019/6/11 17:14
//      * @note 一些值得注意的地方
//      * @example {{{这是一个例子}}}
//      */
//    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
//        val prMapArgs = pr.toMapArgs[PhWorkArgs[_]]
//        val inDF = prMapArgs.getAs[PhDFArgs](inDFName).get.get
//        implicit val sd: PhSparkDriver = sparkObj2
//        sd.setUtil(save2Parquet()).save2Parquet(inDF, path)
//        PhDFArgs(inDF)
//    }
//}
