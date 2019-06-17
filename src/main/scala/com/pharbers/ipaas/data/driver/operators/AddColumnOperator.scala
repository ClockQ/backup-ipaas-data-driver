package com.pharbers.ipaas.data.driver.operators

import org.apache.spark.sql.{Column, DataFrame}
import com.pharbers.ipaas.data.driver.api.work._

/** 添加新列的算子
  *
  * @author clock
  * @version 0.1
  * @since 2019/6/15 18:10
  * @example 默认参数例子
  * {{{
  * inDFName: actionName // 要作用的 DataFrame 名字
  * newColName: newCol // 新增的列名
  * }}}
  */
case class AddColumnOperator(name: String,
                             defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                             pluginLst: Seq[PhPluginTrait2[Column]])
        extends PhOperatorTrait2[DataFrame] {
    /** 要作用的 DataFrame 名字 */
    val inDFName: String = defaultArgs.getAs[PhStringArgs]("inDFName").get.get
    /** 新增的列名列表 */
    val newColName: String = defaultArgs.getAs[PhStringArgs]("newColName").get.get

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[DataFrame] = {
        val inDF = pr.getAs[PhDFArgs](inDFName).get.get
        val func = pluginLst.head.perform(pr).get
        val outDF = inDF.withColumn(newColName, func)

        PhDFArgs(outDF)
    }
}
