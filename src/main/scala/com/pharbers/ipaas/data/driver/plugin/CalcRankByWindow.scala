package com.pharbers.ipaas.data.driver.plugin

import com.pharbers.ipaas.data.driver.api.work.{PhColArgs, PhNoneArgs, PhOperatorTrait, PhWorkArgs}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

case class CalcRankByWindow[T <: Map[String, PhWorkArgs[_]]](args: PhWorkArgs[T]) extends WindowPluginArgs(args) with PhOperatorTrait{
    override val name: String = "calc Descending rank by window "
    override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        val windowYearOnYear = Window.partitionBy(partitionColumnNames.map(x => col(x)): _*).orderBy(col(valueColumnName).desc)

        PhColArgs(rank().over(windowYearOnYear))
    }
}
