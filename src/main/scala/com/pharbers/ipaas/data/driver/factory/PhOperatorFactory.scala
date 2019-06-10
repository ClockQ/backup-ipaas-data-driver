package com.pharbers.ipaas.data.driver.factory

import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.config.yamlModel._

import scala.reflect.runtime.universe

/** 这个类是干啥的
  *
  * @author dcs
  * @param args 参数说明
  * @tparam T 类型参数说明
  * @note 一些值得注意的地方
  */
case class PhOperatorFactory(operator: Operator) extends PhFactoryTrait[PhOperatorTrait] {
    override def inst(): PhOperatorTrait = {
        import scala.collection.JavaConverters._
        val plugin = operator.getPlugin match {
            case _: Plugin => PhFactory.getMethodMirror(operator.getPlugin.getFactory)(operator.getPlugin).asInstanceOf[PhFactoryTrait[PhPluginTrait]].inst()
            case _ => null
        }
        val tmp = PhFactory.getMethodMirror(operator.getReference)(operator.getName, PhMapArgs(operator.getArgs.asScala.map(x => (x._1, PhStringArgs(x._2))).toMap), Seq(plugin))
        tmp.asInstanceOf[PhOperatorTrait]
    }
}
