package com.pharbers.ipaas.data.driver.plugins

import com.pharbers.ipaas.data.driver.api.work._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

/** 计算EI插件
  * @author dcs
  * @note
  */
case class CalcEI () extends PhOperatorTrait{
    override val name: String = "calc ei"
    override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

    /**  EI 今年占比/去年占比
      *   @param   pr   实际类型PhMapArgs. valueColumnName -> 需要计算的列名，dateColName -> 时间列名，partitionColumnNames -> List(用来分区的列名)
      *   @return  PhColArgs
      *   @throws  Exception 传入map中没有规定key时抛出异常
      *   @example df.withColumn("$name", CalcEI().perform(PhMapArgs).get)
      *   @note
      *   @history
      */
    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        val args = pr.toMapArgs[PhWorkArgs[_]]
        val valueColumnName: String = args.get.getOrElse("valueColumnName",throw new Exception("not found valueColumnName")).get.asInstanceOf[String]
        val dateColName: String = args.get.getOrElse("dateColName",throw new Exception("not found dateColName")).get.asInstanceOf[String]
        val partitionColumnNames: List[String] = args.get.getOrElse("partitionColumnNames",throw new Exception("not found partitionColumnNames"))
                .get.asInstanceOf[List[PhStringArgs]].map(x => x.get)
        val windowYearOnYear = Window.partitionBy(partitionColumnNames.map(x => col(x)): _*).orderBy(col(dateColName).cast(IntegerType)).rangeBetween(-100, -100)

        PhColArgs(col(valueColumnName) / first(col(valueColumnName)).over(windowYearOnYear))
    }
}