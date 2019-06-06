package com.pharbers.ipaas.data.driver.plugin

import com.pharbers.ipaas.data.driver.api.work._
import org.apache.spark.sql.functions._

/** 根据年份和月份合并出一列YM
  *
  * @return PhColArgs
  * @throws  Exception 异常类型及说明
  * @history clock
  */
case class MergeYM(name: String = "merge YM by year and month", defaultArgs: PhWorkArgs[_] = PhNoneArgs) extends PhPluginTrait {
    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        PhColArgs(
            when(
                col("MONTH").>=(10), concat(col("YEAR"), col("MONTH"))
            ).otherwise(
                concat(col("YEAR"), lit("0"), col("MONTH"))
            )
        )
    }
}