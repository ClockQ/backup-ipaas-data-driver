package com.pharbers.ipaas.data.driver.factory

import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.config.yamlModel.ActionBean

import scala.reflect.runtime.universe

/** 这个类是干啥的
  *
  * @author dcs
  * @param args 参数说明
  * @tparam T 类型参数说明
  * @note 一些值得注意的地方
  */
case class PhActionFactory(action: ActionBean) extends PhFactoryTrait[PhActionTrait] {
    override def inst(): PhActionTrait = {
        import scala.collection.JavaConverters._
        val operators = action.getOpers.asScala.map(x => PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhFactoryTrait].inst())
        val tmp = PhFactory.getMethodMirror(action.getName)(operators, action.getName)
        tmp.asInstanceOf[PhActionTrait]
    }
}
