package com.pharbers.ipaas.data.driver.operators

import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{Column, DataFrame}
import com.pharbers.ipaas.data.driver.api.work._

/** 使用 Expr 表达式过滤 DataFrame
  *
  * @author clock
  * @version 0.1
  * @since 2019/6/11 16:59
  * @example 默认参数例子
  * {{{
  * inDFName: actionName // 要作用的 DataFrame 名字
  * filter: DATE == 201801 // 过滤的 Expr 表达式
  * }}}
  */
case class ExprFilterOperator(name: String,
                              defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                              pluginLst: Seq[PhPluginTrait2[Column]])
        extends PhOperatorTrait2[DataFrame] {
    /** 要作用的 DataFrame 名字 */
    val inDFName: String = defaultArgs.getAs[PhStringArgs]("inDFName").get.get
    /** filter expr 表达式 */
    val exprFilter: String = defaultArgs.getAs[PhStringArgs]("filter").get.get

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[DataFrame] = {
        val inDF = pr.getAs[PhDFArgs](inDFName).get.get
        val outDF = inDF.filter(expr(exprFilter))
        PhDFArgs(outDF)
    }
}
