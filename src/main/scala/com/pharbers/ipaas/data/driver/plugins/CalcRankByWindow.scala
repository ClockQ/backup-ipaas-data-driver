package com.pharbers.ipaas.data.driver.plugins

import com.pharbers.ipaas.data.driver.api.work.{PhColArgs, PhNoneArgs, PhOperatorTrait, PhStringArgs, PhWorkArgs}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** 计算Rank插件
  * @author dcs
  * @note
  */
case class CalcRankByWindow() extends PhOperatorTrait{
    override val name: String = "calc Descending rank by window "
    override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

    /**  rank 从大到小
      *   @param   pr   实际类型PhMapArgs. valueColumnName -> 需要计算的列名，partitionColumnNames -> List(用来分区的列名)
      *   @return  PhColArgs
      *   @throws  Exception 传入map中没有规定key时抛出异常
      *   @example df.withColumn("$name", CalcMat().CalcRankByWindow(PhMapArgs).get)
      *   @note
      *   @history
      */
    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        val args = pr.toMapArgs[PhWorkArgs[_]]
        val valueColumnName: String = args.get.getOrElse("valueColumnName",throw new Exception("not found valueColumnName")).get.asInstanceOf[String]
        val partitionColumnNames: List[String] = args.get.getOrElse("partitionColumnNames",throw new Exception("not found partitionColumnNames"))
                .get.asInstanceOf[List[PhStringArgs]].map(x => x.get)
        val windowYearOnYear = Window.partitionBy(partitionColumnNames.map(x => col(x)): _*).orderBy(col(valueColumnName).desc)

        PhColArgs(rank().over(windowYearOnYear))
    }
}
