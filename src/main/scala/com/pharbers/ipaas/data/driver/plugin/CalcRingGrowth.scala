package com.pharbers.ipaas.data.driver.plugin

import com.pharbers.ipaas.data.driver.api.work._
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, first, to_date}
import org.apache.spark.sql.types.IntegerType

/** 计算环比插件
  * @author dcs
  * @note
  */
case class CalcRingGrowth [T <: Map[String, PhWorkArgs[_]]]() extends PhOperatorTrait{
    override val name: String = "ring growth"
    override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

    /**  环比 （当月 - 上月） / 上月
      *   @param   pr   实际类型PhMapArgs. valueColumnName -> 需要计算的列名，dateColName -> 时间列名，partitionColumnNames -> List(用来分区的复数列名)
      *   @return  PhColArgs
      *   @throws  Exception 传入map中没有规定key时抛出异常
      *   @example df.CalcRingGrowth("$name", CalcMat().CalcRankByWindow(PhMapArgs).get)
      *   @note
      *   @history
      */
    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[Column] = {
        val args = pr.toMapArgs[PhWorkArgs[_]]
        val valueColumnName: String = args.get.getOrElse("valueColumnName",throw new Exception("not found valueColumnName")).get.asInstanceOf[String]
        val dateColName: String = args.get.getOrElse("dateColName",throw new Exception("not found dateColName")).get.asInstanceOf[String]
        val partitionColumnNames: List[String] = args.get.getOrElse("partitionColumnNames",throw new Exception("not found partitionColumnNames"))
                .get.asInstanceOf[List[PhStringArgs]].map(x => x.get)
        val windowYearOnYear = Window.partitionBy(partitionColumnNames.map(x => col(x)): _*).orderBy(to_date(col(dateColName), "yyyyMM").cast("timestamp").cast("long"))
                .rangeBetween(-86400 * 31, -86400 * 28)

        PhColArgs((col(valueColumnName) - first(col(valueColumnName)).over(windowYearOnYear)) / first(col(valueColumnName)).over(windowYearOnYear))
    }
}
