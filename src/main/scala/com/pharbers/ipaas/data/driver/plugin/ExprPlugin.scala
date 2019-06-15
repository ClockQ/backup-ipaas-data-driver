package com.pharbers.ipaas.data.driver.plugin

import com.pharbers.ipaas.data.driver.api.work._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.expr

/** expr 表达式插件
  *
  * @author clock
  * @version 0.1
  * @since 2019/6/15 18:25
  * @example 默认参数例子
  * {{{
  *     exprString: cast(id as int) // expr 表达式
  * }}}
  */
case class ExprPlugin(name: String,
                      defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                      subPluginLst: Seq[PhPluginTrait2[Column]])
        extends PhPluginTrait2[Column] {
    /** expr 表达式 */
    val exprString: String = defaultArgs.getAs[PhStringArgs]("exprString").get.get

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Column] = {
        PhColArgs(expr(exprString))
    }
}
