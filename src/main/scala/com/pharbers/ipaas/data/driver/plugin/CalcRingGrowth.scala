package com.pharbers.ipaas.data.driver.plugin

import com.pharbers.ipaas.data.driver.api.work._
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, first, to_date}
import org.apache.spark.sql.types.IntegerType

/**
  *
  * @param args
  * @tparam T
  */
case class CalcRingGrowth [T <: Map[String, PhWorkArgs[_]]](args: PhWorkArgs[T]) extends WindowPluginArgs(args) with PhOperatorTrait{
    override val name: String = "ring growth"
    override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[Column] = {
        val windowYearOnYear = Window.partitionBy(partitionColumnNames.map(x => col(x)): _*).orderBy(to_date(col(dateColName), "yyyyMM").cast("timestamp").cast("long"))
                .rangeBetween(-86400 * 31, -86400 * 28)

        PhColArgs((col(valueColumnName) - first(col(valueColumnName)).over(windowYearOnYear)) / first(col(valueColumnName)).over(windowYearOnYear))
    }
}
