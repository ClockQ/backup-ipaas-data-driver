package com.pharbers.ipaas.data.driver.plugin

import com.pharbers.ipaas.data.driver.api.work._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

/**
  *
  * @param args PhMapArgs
  * @tparam T
  */
case class CalcEI [T <: Map[String, PhWorkArgs[_]]](args: PhWorkArgs[T]) extends WindowPluginArgs(args) with PhOperatorTrait{
    override val name: String = "calc ei"
    override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

    /**
      *
      * @param pr 不使用
      * @return PhColArgs
      */
    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        val windowYearOnYear = Window.partitionBy(partitionColumnNames.map(x => col(x)): _*).orderBy(col(dateColName).cast(IntegerType)).rangeBetween(-100, -100)

        PhColArgs(col(valueColumnName) / first(col(valueColumnName)).over(windowYearOnYear))
    }
}
