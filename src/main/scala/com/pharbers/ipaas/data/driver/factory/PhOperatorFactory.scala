package com.pharbers.ipaas.data.driver.factory

import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.config.yamlModel.OperatorBean

import scala.reflect.runtime.universe

/** 这个类是干啥的
  *
  * @author dcs
  * @param args 参数说明
  * @tparam T 类型参数说明
  * @note 一些值得注意的地方
  */
case class PhOperatorFactory(operator: OperatorBean) extends PhFactoryTrait[PhOperatorTrait] {
    override def inst(): PhOperatorTrait = {
        import scala.collection.JavaConverters._
        val plugins = operator.getPlugins.asScala.map(x => PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhFactoryTrait[PhPluginTrait]].inst())
        val tmp = PhFactory.getMethodMirror(operator.getOper)(plugins, operator.getName, PhMapArgs(operator.getArgs.asScala.map(x => (x._1, PhStringArgs(x._2))).toMap))
        tmp.asInstanceOf[PhOperatorTrait]
    }
}
