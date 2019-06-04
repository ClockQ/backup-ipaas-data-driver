package com.pharbers.ipaas.data.driver.plugin

import com.pharbers.ipaas.data.driver.api.work.{PhColArgs, PhNoneArgs, PhOperatorTrait, PhStringArgs, PhWorkArgs}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** 计算占比插件
  * @author dcs
  * @note
  */
case class CalcShare [T <: Map[String, PhWorkArgs[_]]]() extends PhOperatorTrait {
    override val name: String = "calc share"
    override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

    /**  占比
      *   @param   pr   实际类型PhMapArgs. valueColumnName -> 需要计算的列名，dateColName -> 时间列名，partitionColumnNames -> List(用来分区的复数列名)
      *   @return  PhColArgs
      *   @throws  Exception 传入map中没有规定key时抛出异常
      *   @example df.CalcShare("$name", CalcMat().CalcRankByWindow(PhMapArgs).get)
      *   @note
      *   @history
      */
    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        val args = pr.toMapArgs[PhWorkArgs[_]]
        val valueColumnName = args.get.getOrElse("valueColumnName",throw new Exception("not found valueColumnName")).get.asInstanceOf[String]
        val partitionColumnNames = args.get.getOrElse("partitionColumnNames",throw new Exception("not found valueColumnName")).get.asInstanceOf[List[PhStringArgs]].map(x => x.get)
        val windowYearOnYear = Window.partitionBy(partitionColumnNames.map(x => col(x)): _*).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

        PhColArgs(col(valueColumnName) / sum(col(valueColumnName)).over(windowYearOnYear))
    }
}
