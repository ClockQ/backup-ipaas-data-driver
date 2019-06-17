package com.pharbers.ipaas.data.driver.plugins

import com.pharbers.ipaas.data.driver.api.work._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

/** 将年月两列拼接成六位长度的YM（比如把year=2018,month=1或01的数据拼接为201801）
  *
  * @author clock
  * @version 0.1
  * @since 2019/6/17 18:15
  * @example 默认参数例子
  * {{{
  * yearColName: YEAR // 年列名
  * monthColName: MONTH // 月列名
  * }}}
  */
case class MergeYMPlugin(name: String,
                         defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                         subPluginLst: Seq[PhPluginTrait2[Column]])
        extends PhPluginTrait2[Column] {
    /** 年列名 */
    val yearColName: String = defaultArgs.getAs[PhStringArgs]("yearColName").get.get
    /** 月列名 */
    val monthColName: String = defaultArgs.getAs[PhStringArgs]("monthColName").get.get

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Column] = {
        PhColArgs(
            when(
                col(monthColName).>=(10), concat(col(yearColName), col(monthColName))
            ).otherwise(
                concat(col(yearColName), lit("0"), col(monthColName))
            )
        )
    }
}