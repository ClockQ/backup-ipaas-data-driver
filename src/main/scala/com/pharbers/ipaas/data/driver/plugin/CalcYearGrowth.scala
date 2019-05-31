package com.pharbers.ipaas.data.driver.plugin

import com.pharbers.ipaas.data.driver.api.work._
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, first}
import org.apache.spark.sql.types.IntegerType

/**
  *
  * @param args
  * @tparam T
  */
case class CalcYearGrowth[T <: Map[String, PhWorkArgs[_]]]() extends PhOperatorTrait{
    override val name: String = "year growth"
    override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[Column] = {
        val args = pr.toMapArgs[PhWorkArgs[_]]
        val valueColumnName: String = args.get.getOrElse("valueColumnName",throw new Exception("not found valueColumnName")).get.asInstanceOf[String]
        val dateColName: String = args.get.getOrElse("dateColName",throw new Exception("not found dateColName")).get.asInstanceOf[String]
        val partitionColumnNames: List[String] = args.get.getOrElse("partitionColumnNames",throw new Exception("not found partitionColumnNames"))
                .get.asInstanceOf[List[PhStringArgs]].map(x => x.get)
        val windowYearOnYear = Window.partitionBy(partitionColumnNames.map(x => col(x)): _*).orderBy(col(dateColName).cast(IntegerType)).rangeBetween(-100, -100)

        PhColArgs((col(valueColumnName) - first(col(valueColumnName)).over(windowYearOnYear)) / first(col(valueColumnName)).over(windowYearOnYear))
    }
}
