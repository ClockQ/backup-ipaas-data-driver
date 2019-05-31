package com.pharbers.ipaas.data.driver.plugin

import com.pharbers.ipaas.data.driver.api.work._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

/** 这个类是干啥的
  * @author 类创建者 不会出现在文档中
  * @param args 参数说明
  * @tparam T  类型参数说明
  * @note 一些值得注意的地方
  */
case class CalcEI [T <: Map[String, PhWorkArgs[_]]](args: PhWorkArgs[T]) extends WindowPluginArgs(args) with PhOperatorTrait{
    override val name: String = "calc ei"
    override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

    /**  这个方法干啥的
      *   @param   pr   参数说明.
      *   @return  返回值
      *   @throws  Exception 异常类型及说明
      *   @example 这是一个例子
      *   @note 一些值得注意的地方
      *   @history 记录修改历史，暂时在这儿写最后一个修改的是谁
      */
    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        val windowYearOnYear = Window.partitionBy(partitionColumnNames.map(x => col(x)): _*).orderBy(col(dateColName).cast(IntegerType)).rangeBetween(-100, -100)

        PhColArgs(col(valueColumnName) / first(col(valueColumnName)).over(windowYearOnYear))
    }
}