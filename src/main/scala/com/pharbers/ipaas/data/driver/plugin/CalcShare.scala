package com.pharbers.ipaas.data.driver.plugin

import com.pharbers.ipaas.data.driver.api.work.{PhColArgs, PhNoneArgs, PhOperatorTrait, PhStringArgs, PhWorkArgs}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  *
  * @param args
  * @tparam T
  */
class CalcShare [T <: Map[String, PhWorkArgs[_]]](args: PhWorkArgs[T]) extends PhOperatorTrait {
    override val name: String = "calc share"
    override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        val valueColumnName = args.get.getOrElse("valueColumnName",throw new Exception("not found valueColumnName")).get.asInstanceOf[String]
        val partitionColumnNames = args.get.getOrElse("partitionColumnNames",throw new Exception("not found valueColumnName")).get.asInstanceOf[List[PhStringArgs]].map(x => x.get)
        val windowYearOnYear = Window.partitionBy(partitionColumnNames.map(x => col(x)): _*).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

        PhColArgs(col(valueColumnName) / sum(col(valueColumnName)).over(windowYearOnYear))
    }
}
