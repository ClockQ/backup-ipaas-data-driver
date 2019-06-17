package com.pharbers.ipaas.data.driver.plugins

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import com.pharbers.ipaas.data.driver.api.work._

/** when插件
  *
  * @author dcs
  */
case class AddByWhen(name: String,
                     defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                     subPluginLst: Seq[PhPluginTrait2[Column]])
        extends PhPluginTrait2[Column] {

    val mapArgs: PhMapArgs[PhWorkArgs[String]] = defaultArgs.toMapArgs[PhWorkArgs[String]]
    val condition: String = mapArgs.get.getOrElse("condition", throw new Exception("not found condition")).get
    val trueValue: String = mapArgs.get.getOrElse("trueValue", throw new Exception("not found trueValue")).get
    val otherValue: String = mapArgs.get.getOrElse("otherValue", throw new Exception("not found otherValue")).get

    /**
      * @param   pr 实际类型PhMapArgs.condition -> when表达式，trueValue -> 满足条件的赋值，otherValue -> 不满足条件的赋值
      * @return PhColArgs
      * @throws  Exception 异常类型及说明
      * @example df.withColumn("$name", AddByWhen().perform(PhMapArgs).get)
      * @note trueValue 和 otherValue 可以为PhColArgs或者PhStringArgs
      * @history 记录修改历史，暂时在这儿写最后一个修改的是谁
      */
    def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Column] = {
        PhColArgs(when(expr(condition), trueValue).otherwise(otherValue))
    }

}
