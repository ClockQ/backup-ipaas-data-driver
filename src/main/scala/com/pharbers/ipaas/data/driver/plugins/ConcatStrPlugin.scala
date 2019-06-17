package com.pharbers.ipaas.data.driver.plugins

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, concat, lit}
import com.pharbers.ipaas.data.driver.api.work.{PhColArgs, PhMapArgs, PhPluginTrait2, PhStringArgs, PhWorkArgs}

/** 按照指定分隔符拼接多列为字符串插件
  *
  * @author dcs
  * @version 0.1
  * @since 2019/6/12 18:25
  * @example 默认参数例子
  * {{
  * columns: col_1#col_2 // 要拼接的多个列名，用`#`号分割
  * dilimiter: "," //拼接后字符串的分隔符
  * }}}
  */
case class ConcatStrPlugin(name: String,
                           defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                           subPluginLst: Seq[PhPluginTrait2[Column]])
        extends PhPluginTrait2[Column] {
    /** 要拼接的列名列表 */
    val columnList: Array[String] = defaultArgs.getAs[PhStringArgs]("columns").get.get.split("#")
    /** 要拼接的分隔符 */
    val dilimiter: String = defaultArgs.getAs[PhStringArgs]("dilimiter").get.get

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Column] = {
        val concatExpr = columnList.flatMap(x => Array(lit(dilimiter), col(x))).tail
        PhColArgs(concat(concatExpr: _*))
    }
}
