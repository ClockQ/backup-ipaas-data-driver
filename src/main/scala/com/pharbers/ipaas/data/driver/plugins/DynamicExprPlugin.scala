package com.pharbers.ipaas.data.driver.plugins

import com.pharbers.ipaas.data.driver.api.Annotation.Plugin
import com.pharbers.ipaas.data.driver.api.work.{PhColArgs, PhMapArgs, PhPluginTrait, PhStringArgs, PhWorkArgs}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.expr

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/09/09 19:50
  * @note 一些值得注意的地方
  */
@Plugin(name = "dynamic_expr", args = Array("exprString"), msg = "dynamic expr")
case class DynamicExprPlugin(name: String,
                             defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                             subPluginLst: Seq[PhPluginTrait[Column]])(implicit ctx: PhMapArgs[PhWorkArgs[_]])
        extends PhPluginTrait[Column] {
    /** expr 表达式 */
    val exprString: String = defaultArgs.getAs[PhStringArgs]("exprString").get.get
    val replaceString: Map[String, String] = defaultArgs.get.asInstanceOf[PhMapArgs[PhStringArgs]]

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Column] = {
        val exprStr = replaceString.foldLeft(exprString)((l, r) => {
            l.replace(s"#${r._1}#", r._2)
        })

        PhColArgs(expr(exprStr))
    }
}