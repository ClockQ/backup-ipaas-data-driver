package com.pharbers.ipaas.data.driver.plugin

import com.pharbers.ipaas.data.driver.api.work._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

case class CalcMat [T <: Map[String, PhWorkArgs[_]]]() extends WindowPluginArgs(args) with PhOperatorTrait{
    override val name: String = "calc mat"
    override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        val args = pr.toMapArgs[PhWorkArgs[_]]
        val valueColumnName: String = args.get.getOrElse("valueColumnName",throw new Exception("not found valueColumnName")).get.asInstanceOf[String]
        val dateColName: String = args.get.getOrElse("dateColName",throw new Exception("not found dateColName")).get.asInstanceOf[String]
        val partitionColumnNames: List[String] = args.get.getOrElse("partitionColumnNames",throw new Exception("not found partitionColumnNames"))
                .get.asInstanceOf[List[PhStringArgs]].map(x => x.get)
        val windowYearOnYear = Window.partitionBy(partitionColumnNames.map(x => col(x)): _*).orderBy(col(dateColName).cast(IntegerType)).rangeBetween(-99, 0)

        PhColArgs(sum(col(valueColumnName)).over(windowYearOnYear))
    }
}
