package com.pharbers.ipaas.data.driver.plugins

import com.pharbers.ipaas.data.driver.api.Annotation.Plugin
import com.pharbers.ipaas.data.driver.api.work.{PhColArgs, PhMapArgs, PhPluginTrait, PhStringArgs, PhWorkArgs}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/09/06 18:28
  * @note 一些值得注意的地方
  */
@Plugin(name = "expr lit", args = Array("exprString"), msg = "expr only a string not column")
case class LitPlugin(name: String,
                     defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                     subPluginLst: Seq[PhPluginTrait[Column]])(implicit ctx: PhMapArgs[PhWorkArgs[_]])
        extends PhPluginTrait[Column] {
    /** expr 表达式 */
    val exprString: String = defaultArgs.getAs[PhStringArgs]("exprString").get.get

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Column] = {
        PhColArgs(lit(exprString))
    }
}