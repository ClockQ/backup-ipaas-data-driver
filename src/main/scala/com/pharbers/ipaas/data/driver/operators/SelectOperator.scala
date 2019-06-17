package com.pharbers.ipaas.data.driver.operators

import org.apache.spark.sql.{Column, DataFrame}
import com.pharbers.ipaas.data.driver.api.work._

/** 对数据集保留指定列名
  *
  * @author clock
  * @version 0.1
  * @since 2019/6/17 17:00
  * @example 默认参数例子
  * {{{
  *  inDFName: actionName // 要作用的 DataFrame 名字
  *  selects: col_1#col_2 // 选择的列名，用`#`号分割
  * }}}
  */
case class SelectOperator(name: String,
                          defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                          pluginLst: Seq[PhPluginTrait2[Column]]) extends PhOperatorTrait2[DataFrame] {
    /** 要作用的 DataFrame 名字 */
    val inDFName: String = defaultArgs.getAs[PhStringArgs]("inDFName").get.get
    /** 选择的列名列表 */
    val selectLst: Array[String] = defaultArgs.getAs[PhStringArgs]("selects").get.get.split("#")

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[DataFrame] = {
        val inDF = pr.getAs[PhDFArgs](inDFName).get.get
        val outDF = inDF.selectExpr(selectLst: _*)

        PhDFArgs(outDF)
    }
}
