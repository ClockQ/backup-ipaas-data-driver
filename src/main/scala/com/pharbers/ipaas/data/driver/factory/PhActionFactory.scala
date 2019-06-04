package com.pharbers.ipaas.data.driver.factory

import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.config.yamlModel.ActionBean

import scala.reflect.runtime.universe

/** Action工厂
  *
  * @author dcs
  * @param action 持久化的配置对象
  * @note 一些值得注意的地方
  */
case class PhActionFactory(action: ActionBean) extends PhFactoryTrait[PhActionTrait] {

    /**  生成Action实例
      *   @return  PhActionTrait
      *   @throws  Exception
      *   @note
      *   @history
      */
    override def inst(): PhActionTrait = {
        import scala.collection.JavaConverters._
        val operators = action.getOpers.asScala.map(x => PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhOperatorFactory].inst()).toList
        val tmp = PhFactory.getMethodMirror(action.getReference)(operators, action.getName, PhMapArgs(action.getArgs.asScala.map(x => (x._1, PhStringArgs(x._2))).toMap))
        tmp.asInstanceOf[PhActionTrait]
    }
}
