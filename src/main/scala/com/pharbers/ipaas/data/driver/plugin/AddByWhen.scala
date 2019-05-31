package com.pharbers.ipaas.data.driver.plugin

import com.pharbers.ipaas.data.driver.api.work._
import org.apache.spark.sql.functions._

/**when插件
  * @author dcs
  */
case class AddByWhen() extends PhOperatorTrait{
    override val name: String = "add column by when"
    override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

	/**
	  *   @param   pr  实际类型PhMapArgs.condition -> when表达式，trueValue -> 满足条件的赋值，otherValue -> 不满足条件的赋值
	  *   @return   PhColArgs
	  *   @throws  Exception 异常类型及说明
	  *   @example df.withColumn("$name", AddByWhen().perform(PhMapArgs).get)
	  *   @note trueValue 和 otherValue 可以为PhColArgs或者PhStringArgs
	  *   @history 记录修改历史，暂时在这儿写最后一个修改的是谁
	  */
    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        val args = pr.toMapArgs[PhWorkArgs[_]]
        val condition = args.get.getOrElse("condition",throw new Exception("not found condition")).get.asInstanceOf[String]
        val trueValue = args.get.getOrElse("trueValue",throw new Exception("not found trueValue")).get
        val otherValue = args.get.getOrElse("otherValue",throw new Exception("not found otherValue")).get

        PhColArgs(when(expr(condition), trueValue).otherwise(otherValue))
    }
}
